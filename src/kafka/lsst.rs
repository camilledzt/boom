use crate::{kafka::base::AlertConsumer, utils::enums::Survey};
use tracing::instrument;

#[derive(Clone)]
pub struct LsstAlertConsumer {
    output_queue: String,
    simulated: bool,
}

impl LsstAlertConsumer {
    #[instrument]
    pub fn new(output_queue: Option<&str>, simulated: bool) -> Self {
        let base = output_queue.unwrap_or("LSST_alerts_packets_queue");
        let output_queue = sim_suffix(base);

        LsstAlertConsumer {
            output_queue,
            simulated,
        }
    }
}

/// Appends `_{BOOM_SIM_ID}` to `name` when the env var is set and non-empty.
pub fn sim_suffix(name: &str) -> String {
    match std::env::var("BOOM_SIM_ID").ok().filter(|s| !s.is_empty()) {
        Some(id) => format!("{}_{}", name, id),
        None => name.to_string(),
    }
}

#[async_trait::async_trait]
impl AlertConsumer for LsstAlertConsumer {
    fn topic_names(&self, _timestamp: i64) -> Vec<String> {
        if self.simulated {
            vec![sim_suffix("alerts-simulated")]
        } else {
            vec!["lsst-alerts-v11".to_string()]
        }
    }
    fn subscription_topics(&self, _timestamp: i64, _window_days: u64) -> Vec<String> {
        // LSST uses a single static topic (not date-partitioned), so there is
        // nothing to roll over: subscribe to the literal topic name.
        self.topic_names(0)
    }
    fn output_queue(&self) -> String {
        self.output_queue.clone()
    }
    fn survey(&self) -> &'static str {
        Survey::Lsst.as_str()
    }
}
