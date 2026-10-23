use super::*;

#[test]
fn uri_encode_preserves_unreserved_and_slashes() {
    assert_eq!(uri_encode("simple-key_1.json", false), "simple-key_1.json");
    // Slashes preserved when encode_slash is false.
    assert_eq!(uri_encode("a/b/c.json", false), "a/b/c.json");
    // Spaces and other bytes percent-encoded, uppercase hex.
    assert_eq!(uri_encode("a b", false), "a%20b");
}

#[test]
fn uri_encode_can_encode_slashes() {
    assert_eq!(uri_encode("a/b", true), "a%2Fb");
}

#[test]
fn scheme_prefix_handles_https_and_bare_hosts() {
    assert_eq!(
        scheme_prefix("https://acct.r2.cloudflarestorage.com/"),
        "https://acct.r2.cloudflarestorage.com"
    );
    assert_eq!(
        scheme_prefix("acct.r2.cloudflarestorage.com"),
        "https://acct.r2.cloudflarestorage.com"
    );
}

#[test]
fn trim_scheme_recovers_host() {
    let parsed = url::trim_scheme("https://acct.r2.cloudflarestorage.com/bucket");
    assert_eq!(parsed.host, "acct.r2.cloudflarestorage.com");
}

#[test]
fn parse_list_objects_reads_each_contents_key_size_and_time() {
    let xml = "<?xml version=\"1.0\"?><ListBucketResult>\
        <Name>bucket</Name><Prefix>media/</Prefix><KeyCount>2</KeyCount>\
        <Contents><Key>media/runs/a/proof/title.png</Key>\
        <LastModified>2026-10-19T12:03:04.000Z</LastModified>\
        <ETag>&quot;abc&quot;</ETag><Size>1234</Size>\
        <StorageClass>STANDARD</StorageClass></Contents>\
        <Contents><Key>media/runs/b/asset/regenerated-0.png</Key><Size>7</Size></Contents>\
        </ListBucketResult>";
    assert_eq!(
        parse_list_objects(xml),
        vec![
            PublicObject {
                key: "media/runs/a/proof/title.png".to_string(),
                bytes: 1234,
                last_modified: Some(time::macros::datetime!(2026-10-19 12:03:04 UTC)),
            },
            PublicObject {
                key: "media/runs/b/asset/regenerated-0.png".to_string(),
                bytes: 7,
                last_modified: None,
            },
        ],
    );
    // An empty listing yields no objects.
    assert!(parse_list_objects("<ListBucketResult></ListBucketResult>").is_empty());
}

#[test]
fn parse_list_objects_decodes_xml_entities_in_keys() {
    let xml = "<Contents><Key>a&amp;b/c.json</Key><Size>0</Size></Contents>";
    assert_eq!(parse_list_objects(xml)[0].key, "a&b/c.json");
}

#[test]
fn continuation_token_present_only_when_truncated() {
    let truncated = "<IsTruncated>true</IsTruncated>\
        <NextContinuationToken>1abc/def=</NextContinuationToken>";
    assert_eq!(
        parse_next_continuation_token(truncated),
        Some("1abc/def=".to_string())
    );
    // No token element → the listing is complete.
    assert_eq!(
        parse_next_continuation_token("<IsTruncated>false</IsTruncated>"),
        None
    );
    // An empty token element is treated as complete, not an empty page cursor.
    assert_eq!(
        parse_next_continuation_token("<NextContinuationToken></NextContinuationToken>"),
        None
    );
}

/// The example credentials of AWS's SigV4 documentation for S3.
const AWS_SECRET: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
/// The empty payload's SHA-256, as the examples sign it.
const EMPTY: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// Sign one of AWS's documented example requests (examplebucket, us-east-1,
/// 2013-05-24) and answer the signature.
fn aws_example(
    method: &str,
    canonical_uri: &str,
    query: &[(&str, String)],
    extra: &[(&str, &str)],
    payload_hash: &str,
) -> String {
    let mut headers: Vec<(String, String)> = vec![
        (
            "host".to_string(),
            "examplebucket.s3.amazonaws.com".to_string(),
        ),
        ("x-amz-content-sha256".to_string(), payload_hash.to_string()),
        ("x-amz-date".to_string(), "20130524T000000Z".to_string()),
    ];
    headers.extend(
        extra
            .iter()
            .map(|(name, value)| ((*name).to_string(), (*value).to_string())),
    );
    headers.sort();
    let (canonical, _) = canonical_request(
        method,
        canonical_uri,
        &canonical_query(query),
        &headers,
        payload_hash,
    );
    let (scope, signature) =
        sign_v4(AWS_SECRET, "us-east-1", "20130524T000000Z", &canonical).unwrap();
    assert_eq!(scope, "20130524/us-east-1/s3/aws4_request");
    signature
}

#[test]
fn sigv4_matches_the_aws_get_object_example() {
    assert_eq!(
        aws_example("GET", "/test.txt", &[], &[("range", "bytes=0-9")], EMPTY),
        "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
    );
}

#[test]
fn sigv4_matches_the_aws_put_object_example() {
    // "Welcome to Amazon S3." with a Date header and a storage class, both signed.
    assert_eq!(
        aws_example(
            "PUT",
            &format!("/{}", encode_key("test$file.text")),
            &[],
            &[
                ("date", "Fri, 24 May 2013 00:00:00 GMT"),
                ("x-amz-storage-class", "REDUCED_REDUNDANCY"),
            ],
            "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072",
        ),
        "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"
    );
}

#[test]
fn sigv4_matches_the_aws_subresource_example() {
    assert_eq!(
        aws_example("GET", "/", &[("lifecycle", String::new())], &[], EMPTY),
        "fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543"
    );
}

#[test]
fn sigv4_matches_the_aws_list_objects_example() {
    // The parameters are given out of order; the canonical query sorts them.
    assert_eq!(
        aws_example(
            "GET",
            "/",
            &[("prefix", "J".to_string()), ("max-keys", "2".to_string())],
            &[],
            EMPTY,
        ),
        "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"
    );
}

#[test]
fn the_canonical_query_encodes_slashes_and_valueless_subresources() {
    assert_eq!(
        canonical_query(&[
            ("prefix", "media/runs/".to_string()),
            ("list-type", "2".to_string()),
        ]),
        "list-type=2&prefix=media%2Fruns%2F"
    );
    assert_eq!(canonical_query(&[("uploads", String::new())]), "uploads=");
}

#[test]
fn the_secret_is_left_out_of_debug() {
    let config = R2Config {
        account_id: "acct".to_string(),
        bucket: "bucket".to_string(),
        access_key_id: "AKID".to_string(),
        secret_access_key: "very-secret".to_string(),
        endpoint: "https://acct.r2.cloudflarestorage.com".to_string(),
        region: "auto".to_string(),
    };
    assert!(!format!("{config:?}").contains("very-secret"));
}

#[test]
fn the_config_resolves_from_its_variables() {
    let vars = std::collections::HashMap::from([
        ("TCAB_R2_ACCOUNT_ID", "acct"),
        ("TCAB_R2_BUCKET", "bucket"),
        ("TCAB_R2_ACCESS_KEY_ID", "id"),
        ("TCAB_R2_SECRET_ACCESS_KEY", "secret"),
    ]);
    let lookup = |key: &str| vars.get(key).map(|value| (*value).to_string());
    let config = R2Config::from_lookup(lookup).unwrap();
    assert_eq!(config.endpoint, "https://acct.r2.cloudflarestorage.com");
    assert_eq!(config.region, "auto");
    assert!(
        R2Config::from_lookup(|key| (key != "TCAB_R2_BUCKET").then(|| lookup(key)).flatten())
            .is_none()
    );
}

#[test]
fn an_upload_id_is_read_from_its_element() {
    assert_eq!(
        xml_element(
            "<InitiateMultipartUploadResult><UploadId>a&amp;b</UploadId></InitiateMultipartUploadResult>",
            "UploadId"
        )
        .as_deref(),
        Some("a&b")
    );
    assert_eq!(xml_element("<X/>", "UploadId"), None);
}

#[test]
fn xml_escape_is_the_inverse_of_unescape() {
    // Text carrying every predefined entity round-trips, which is what keeps a
    // `CompleteMultipartUpload` body naming the same part the store returned.
    let key = "snapshots/a&b/<c>/\"d\"/'e'.json";
    assert_eq!(xml_unescape(&xml_escape(key)), key);
}

#[test]
fn xml_escape_does_not_double_escape_ampersands() {
    // `&` must be replaced first; escaping it last would turn the `&` of `&lt;`
    // into `&amp;lt;` and name the wrong part.
    assert_eq!(xml_escape("a&b"), "a&amp;b");
    assert_eq!(xml_escape("a<b"), "a&lt;b");
    assert_eq!(xml_escape("a&<b"), "a&amp;&lt;b");
}
