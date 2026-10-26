use std::time::Duration;

use super::*;

#[test]
fn a_new_service_is_ready_and_a_drained_one_is_not() {
    let readiness = Readiness::new();
    assert!(readiness.is_ready());
    let clone = readiness.clone();
    clone.mark_draining();
    clone.mark_draining();
    assert!(readiness.is_draining());
    assert!(!readiness.is_ready());
}

#[test]
fn only_a_terminate_waits_out_the_drain_delay() {
    assert_eq!(Stop::Terminate.drain_delay(), DRAIN_DELAY);
    assert_eq!(DRAIN_DELAY, Duration::from_secs(5));
    assert_eq!(Stop::Interrupt.drain_delay(), Duration::ZERO);
}

#[test]
fn the_stop_deadline_falls_inside_the_termination_grace() {
    // The grace is 300 s, of which the `preStop` hook spends the first 5.
    assert!(STOP_DEADLINE + Duration::from_secs(5) < Duration::from_secs(300));
    assert!(STOP_DEADLINE > DRAIN_DELAY);
}

/// A stop request answered by the returned sender, as a signal would.
fn requested_by_hand() -> (tokio::sync::oneshot::Sender<Stop>, StopRequest) {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let stop = StopRequest::spawn(async move { receiver.await.unwrap() });
    (sender, stop)
}

#[tokio::test(start_paused = true)]
async fn every_clone_sees_one_request_even_after_it_arrived() {
    let (sender, stop) = requested_by_hand();
    let early = stop.clone();
    let waiting = tokio::spawn(async move { early.requested().await.0 });
    sender.send(Stop::Terminate).unwrap();
    assert_eq!(waiting.await.unwrap(), Stop::Terminate);
    // A waiter that comes later is answered at once.
    assert_eq!(stop.requested().await.0, Stop::Terminate);
}

#[tokio::test(start_paused = true)]
async fn a_terminate_fails_readiness_at_once_and_resolves_after_the_delay() {
    let readiness = Readiness::new();
    let (sender, stop) = requested_by_hand();
    let mut draining = Box::pin(drain(readiness.clone(), stop));
    assert!(futures_util::poll!(draining.as_mut()).is_pending());
    assert!(readiness.is_ready(), "readiness failed before the stop");
    sender.send(Stop::Terminate).unwrap();
    // Let the request reach the drain, which marks draining and parks on the delay.
    tokio::time::timeout(Duration::from_millis(1), draining.as_mut())
        .await
        .expect_err("resolved before its delay");
    assert!(readiness.is_draining());
    tokio::time::advance(DRAIN_DELAY - Duration::from_millis(5)).await;
    assert!(futures_util::poll!(draining.as_mut()).is_pending());
    tokio::time::advance(Duration::from_millis(5)).await;
    assert_eq!(draining.await, Stop::Terminate);
}

#[tokio::test(start_paused = true)]
async fn an_interrupt_resolves_without_a_delay() {
    let readiness = Readiness::new();
    let (sender, stop) = requested_by_hand();
    sender.send(Stop::Interrupt).unwrap();
    let started = tokio::time::Instant::now();
    assert_eq!(drain(readiness.clone(), stop).await, Stop::Interrupt);
    assert_eq!(started.elapsed(), Duration::ZERO);
    assert!(readiness.is_draining());
}

#[tokio::test(start_paused = true)]
async fn the_deadline_falls_its_length_after_the_request() {
    let (sender, stop) = requested_by_hand();
    let mut deadline = Box::pin(deadline(stop));
    // No request, no deadline, however long it waits.
    tokio::time::timeout(STOP_DEADLINE * 2, deadline.as_mut())
        .await
        .expect_err("a deadline with no stop requested");
    sender.send(Stop::Terminate).unwrap();
    let requested = tokio::time::Instant::now();
    deadline.await;
    assert_eq!(requested.elapsed(), STOP_DEADLINE);
}
