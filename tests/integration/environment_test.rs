use crate::common::TestEnvironment;

#[tokio::test]
async fn environment_create_test() {
    let _ = TestEnvironment::create().await;
}
