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

#[tokio::test(start_paused = true)]
async fn a_terminate_fails_readiness_at_once_and_resolves_after_the_delay() {
    let readiness = Readiness::new();
    let mut draining = Box::pin(drain(readiness.clone(), async { Stop::Terminate }));
    // The first poll marks draining and parks on the delay.
    assert!(
        futures_util::poll!(draining.as_mut()).is_pending(),
        "resolved before its delay"
    );
    assert!(readiness.is_draining());
    tokio::time::advance(DRAIN_DELAY - Duration::from_millis(1)).await;
    assert!(futures_util::poll!(draining.as_mut()).is_pending());
    tokio::time::advance(Duration::from_millis(1)).await;
    assert_eq!(draining.await, Stop::Terminate);
}

#[tokio::test(start_paused = true)]
async fn an_interrupt_resolves_without_a_delay() {
    let readiness = Readiness::new();
    let mut draining = Box::pin(drain(readiness.clone(), async { Stop::Interrupt }));
    assert_eq!(
        futures_util::poll!(draining.as_mut()),
        std::task::Poll::Ready(Stop::Interrupt)
    );
    assert!(readiness.is_draining());
}
