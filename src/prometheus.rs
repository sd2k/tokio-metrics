use std::sync::{Arc, RwLock};

use prometheus_client::{
    encoding::text::{Encode, EncodeMetric, Encoder, SendEncodeMetric},
    metrics::MetricType,
    registry::Registry,
};

use crate::{RuntimeMetrics, RuntimeMetricsIter, RuntimeMonitor};

fn accumulate_metrics(current: &mut RuntimeMetrics, new: RuntimeMetrics) {
    current.elapsed = new.elapsed;

    // Gauges.
    // New values for gauges should just overwrite the old ones.
    current.workers_count = new.workers_count;
    current.injection_queue_depth = new.injection_queue_depth;

    // Counters.
    current.min_park_count += new.min_park_count;
    current.max_park_count += new.max_park_count;
    current.total_park_count += new.total_park_count;

    current.min_noop_count += new.min_noop_count;
    current.max_noop_count += new.max_noop_count;
    current.total_noop_count += new.total_noop_count;

    current.min_steal_count += new.min_steal_count;
    current.max_steal_count += new.max_steal_count;
    current.total_steal_count += new.total_steal_count;

    current.num_remote_schedules += new.num_remote_schedules;

    current.min_local_schedule_count += new.min_local_schedule_count;
    current.max_local_schedule_count += new.max_local_schedule_count;
    current.total_local_schedule_count += new.total_local_schedule_count;

    current.min_overflow_count += new.min_overflow_count;
    current.max_overflow_count += new.max_overflow_count;
    current.total_overflow_count += new.total_overflow_count;

    current.min_polls_count += new.min_polls_count;
    current.max_polls_count += new.max_polls_count;
    current.total_polls_count += new.total_polls_count;

    current.min_busy_duration += new.min_busy_duration;
    current.max_busy_duration += new.max_busy_duration;
    current.total_busy_duration += new.total_busy_duration;

    current.min_local_queue_depth += new.min_local_queue_depth;
    current.max_local_queue_depth += new.max_local_queue_depth;
    current.total_local_queue_depth += new.total_local_queue_depth;
}

struct CachedMonitor {
    iter: RuntimeMetricsIter,
    current: RuntimeMetrics,
}

impl CachedMonitor {
    fn new(monitor: &RuntimeMonitor) -> Self {
        let mut iter = monitor.intervals();
        let current = iter.next().unwrap();
        Self { iter, current }
    }

    fn refresh(&mut self) {
        accumulate_metrics(&mut self.current, self.iter.next().unwrap());
    }

    fn get(&self) -> &RuntimeMetrics {
        &self.current
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Encode)]
struct MinMaxTotalLabels {
    measurement: Measurement,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Encode)]
#[allow(non_camel_case_types)]
enum Measurement {
    min,
    max,
    total,
}

macro_rules! metric_struct {
    ( $struct_name:ident, $metric_name:ident, $description:expr, $metric_type:expr, first ) => {
        struct $struct_name(Arc<RwLock<CachedMonitor>>);

        impl $struct_name {
            fn name(&self) -> &'static str {
                stringify!($metric_name)
            }

            fn description(&self) -> &'static str {
                $description
            }
        }

        impl EncodeMetric for $struct_name {
            fn encode(&self, mut encoder: Encoder) -> Result<(), std::io::Error> {
                self.0.write().unwrap().refresh();
                encoder
                    .no_suffix()?
                    .no_bucket()?
                    .encode_value(self.0.read().unwrap().get().$metric_name as u64)?
                    .no_exemplar()
            }

            fn metric_type(&self) -> MetricType {
                $metric_type
            }
        }
    };
    ( $struct_name:ident, $metric_name:ident, $description:expr, $metric_type:expr$(,)? ) => {
        struct $struct_name(Arc<RwLock<CachedMonitor>>);

        impl $struct_name {
            fn name(&self) -> &'static str {
                stringify!($metric_name)
            }

            fn description(&self) -> &'static str {
                $description
            }
        }

        impl EncodeMetric for $struct_name {
            fn encode(&self, mut encoder: Encoder) -> Result<(), std::io::Error> {
                let metrics = self.0.read().unwrap();
                encoder
                    .no_suffix()?
                    .no_bucket()?
                    .encode_value(metrics.get().$metric_name as u64)?
                    .no_exemplar()
            }

            fn metric_type(&self) -> MetricType {
                $metric_type
            }
        }
    };
    ( $struct_name:ident, $metric_name:ident, $description:expr, $metric_type:expr, $extract:expr$(,)?) => {
        struct $struct_name(Arc<RwLock<CachedMonitor>>);

        impl $struct_name {
            fn name(&self) -> &'static str {
                stringify!($metric_name)
            }

            fn description(&self) -> &'static str {
                $description
            }
        }

        impl EncodeMetric for $struct_name {
            fn encode(&self, mut encoder: Encoder) -> Result<(), std::io::Error> {
                let metrics = self.0.read().unwrap();
                encoder
                    .no_suffix()?
                    .no_bucket()?
                    .encode_value($extract(metrics.get()))?
                    .no_exemplar()
            }

            fn metric_type(&self) -> MetricType {
                $metric_type
            }
        }
    };
    ( $struct_name:ident, $metric_name:ident, $description:expr, $metric_type:expr, $extract_min:expr, $extract_max:expr, $extract_total:expr$(,)?) => {
        struct $struct_name(Arc<RwLock<CachedMonitor>>);

        impl $struct_name {
            fn name(&self) -> &'static str {
                stringify!($metric_name)
            }

            fn description(&self) -> &'static str {
                $description
            }
        }

        impl EncodeMetric for $struct_name {
            fn encode(&self, mut encoder: Encoder) -> Result<(), std::io::Error> {
                let metrics = self.0.read().unwrap();
                encoder
                    .with_label_set(&MinMaxTotalLabels {
                        measurement: Measurement::min,
                    })
                    .no_suffix()?
                    .no_bucket()?
                    .encode_value($extract_min(metrics.get()))?
                    .no_exemplar()?;
                encoder
                    .with_label_set(&MinMaxTotalLabels {
                        measurement: Measurement::max,
                    })
                    .no_suffix()?
                    .no_bucket()?
                    .encode_value($extract_max(metrics.get()))?
                    .no_exemplar()?;
                encoder
                    .with_label_set(&MinMaxTotalLabels {
                        measurement: Measurement::total,
                    })
                    .no_suffix()?
                    .no_bucket()?
                    .encode_value($extract_total(metrics.get()))?
                    .no_exemplar()
            }

            fn metric_type(&self) -> MetricType {
                $metric_type
            }
        }
    };
}

metric_struct!(
    WorkersCount,
    workers_count,
    "The number of worker threads",
    MetricType::Gauge,
    first
);
metric_struct!(
    ParkCount,
    park_count,
    "The number of times worker threads parked",
    MetricType::Counter,
    |metrics: &RuntimeMetrics| metrics.min_park_count,
    |metrics: &RuntimeMetrics| metrics.max_park_count,
    |metrics: &RuntimeMetrics| metrics.total_park_count,
);
metric_struct!(
    NoopCount,
    noop_count,
    "The number of times worker threads unparked but performed no work before parking again",
    MetricType::Counter,
    |metrics: &RuntimeMetrics| metrics.min_noop_count,
    |metrics: &RuntimeMetrics| metrics.max_noop_count,
    |metrics: &RuntimeMetrics| metrics.total_noop_count,
);
metric_struct!(
    StealCount,
    steal_count,
    "The number of times worker threads stole tasks from another worker thread",
    MetricType::Counter,
    |metrics: &RuntimeMetrics| metrics.min_steal_count,
    |metrics: &RuntimeMetrics| metrics.max_steal_count,
    |metrics: &RuntimeMetrics| metrics.total_steal_count,
);
metric_struct!(
    RemoteScheduleCount,
    remote_schedule_count,
    "The number of tasks scheduled from **outside** of the runtime",
    MetricType::Counter,
    |metrics: &RuntimeMetrics| metrics.num_remote_schedules,
);
metric_struct!(
    LocalScheduleCount,
    local_schedule_count,
    "The number of tasks scheduled from worker threads",
    MetricType::Counter,
    |metrics: &RuntimeMetrics| metrics.min_local_schedule_count,
    |metrics: &RuntimeMetrics| metrics.max_local_schedule_count,
    |metrics: &RuntimeMetrics| metrics.total_local_schedule_count,
);
metric_struct!(
    OverflowCount,
    overflow_count,
    "The number of times worker threads saturated their local queues",
    MetricType::Counter,
    |metrics: &RuntimeMetrics| metrics.min_overflow_count,
    |metrics: &RuntimeMetrics| metrics.max_overflow_count,
    |metrics: &RuntimeMetrics| metrics.total_overflow_count,
);
metric_struct!(
    PollsCount,
    polls_count,
    "The number of tasks that have been polled across all worker threads",
    MetricType::Counter,
    |metrics: &RuntimeMetrics| metrics.min_polls_count,
    |metrics: &RuntimeMetrics| metrics.max_polls_count,
    |metrics: &RuntimeMetrics| metrics.total_polls_count,
);
metric_struct!(
    BusyDuration,
    busy_duration_seconds,
    "The amount of time worker threads were busy",
    MetricType::Counter,
    |metrics: &RuntimeMetrics| metrics.min_busy_duration.as_secs_f64(),
    |metrics: &RuntimeMetrics| metrics.max_busy_duration.as_secs_f64(),
    |metrics: &RuntimeMetrics| metrics.total_busy_duration.as_secs_f64(),
);
metric_struct!(
    InjectionQueueDepth,
    injection_queue_depth,
    "The number of tasks currently scheduled in the runtime's injection queue",
    MetricType::Gauge,
);
metric_struct!(
    LocalQueueDepth,
    local_queue_depth,
    "The total number of tasks currently scheduled in workers' local queues",
    MetricType::Counter,
    |metrics: &RuntimeMetrics| metrics.min_local_queue_depth as u64,
    |metrics: &RuntimeMetrics| metrics.max_local_queue_depth as u64,
    |metrics: &RuntimeMetrics| metrics.total_local_queue_depth as u64,
);
metric_struct!(
    MeanPollsPerPark,
    mean_polls_per_park,
    "The mean number of polls per park",
    MetricType::Gauge,
    |metrics: &RuntimeMetrics| metrics.mean_polls_per_park(),
);
metric_struct!(
    BusyRatio,
    busy_ratio,
    "The proportion of time spent polling for tasks",
    MetricType::Gauge,
    |metrics: &RuntimeMetrics| metrics.busy_ratio(),
);

pub struct PrometheusCollector {
    workers_count: WorkersCount,
    park_count: ParkCount,
    noop_count: NoopCount,
    steal_count: StealCount,
    remote_schedule_count: RemoteScheduleCount,
    local_schedule_count: LocalScheduleCount,
    overflow_count: OverflowCount,
    polls_count: PollsCount,
    busy_duration: BusyDuration,
    injection_queue_depth: InjectionQueueDepth,
    local_queue_depth: LocalQueueDepth,
    mean_polls_per_park: MeanPollsPerPark,
    busy_ratio: BusyRatio,
}

macro_rules! register {
    ( $registry:expr, $metric:expr ) => {
        $registry.register(
            $metric.name(),
            $metric.description(),
            Box::new($metric) as Box<dyn SendEncodeMetric>,
        );
    };
}

impl PrometheusCollector {
    pub fn new(monitor: &RuntimeMonitor) -> Self {
        let cached = Arc::new(RwLock::new(CachedMonitor::new(monitor)));
        Self {
            workers_count: WorkersCount(Arc::clone(&cached)),
            park_count: ParkCount(Arc::clone(&cached)),
            noop_count: NoopCount(Arc::clone(&cached)),
            steal_count: StealCount(Arc::clone(&cached)),
            remote_schedule_count: RemoteScheduleCount(Arc::clone(&cached)),
            local_schedule_count: LocalScheduleCount(Arc::clone(&cached)),
            overflow_count: OverflowCount(Arc::clone(&cached)),
            polls_count: PollsCount(Arc::clone(&cached)),
            busy_duration: BusyDuration(Arc::clone(&cached)),
            injection_queue_depth: InjectionQueueDepth(Arc::clone(&cached)),
            local_queue_depth: LocalQueueDepth(Arc::clone(&cached)),
            mean_polls_per_park: MeanPollsPerPark(Arc::clone(&cached)),
            busy_ratio: BusyRatio(cached),
        }
    }

    pub fn register(self, registry: &mut Registry<Box<dyn SendEncodeMetric>>) {
        register!(registry, self.workers_count);
        register!(registry, self.park_count);
        register!(registry, self.noop_count);
        register!(registry, self.steal_count);
        register!(registry, self.remote_schedule_count);
        register!(registry, self.local_schedule_count);
        register!(registry, self.overflow_count);
        register!(registry, self.polls_count);
        register!(registry, self.busy_duration);
        register!(registry, self.injection_queue_depth);
        register!(registry, self.local_queue_depth);
        register!(registry, self.mean_polls_per_park);
        register!(registry, self.busy_ratio);
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    #[tokio::test]
    async fn can_register() {
        let handle = tokio::runtime::Handle::current();
        let monitor = crate::RuntimeMonitor::new(&handle);
        tokio::spawn(async {
            for _ in 0..25 {
                tokio::task::yield_now().await;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let collector = crate::PrometheusCollector::new(&monitor);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut registry = prometheus_client::registry::Registry::default();
        collector.register(&mut registry);
        let mut buffer = vec![];
        prometheus_client::encoding::text::encode(&mut buffer, &registry).unwrap();
        let expected = r#"# HELP workers_count The number of worker threads.
# TYPE workers_count gauge
workers_count 1
# HELP park_count The number of times worker threads parked.
# TYPE park_count counter
park_count{measurement="min"} 0
park_count{measurement="max"} 0
park_count{measurement="total"} 0
# HELP noop_count The number of times worker threads unparked but performed no work before parking again.
# TYPE noop_count counter
noop_count{measurement="min"} 0
noop_count{measurement="max"} 0
noop_count{measurement="total"} 0
# HELP steal_count The number of times worker threads stole tasks from another worker thread.
# TYPE steal_count counter
steal_count{measurement="min"} 0
steal_count{measurement="max"} 0
steal_count{measurement="total"} 0
# HELP remote_schedule_count The number of tasks scheduled from **outside** of the runtime.
# TYPE remote_schedule_count counter
remote_schedule_count 0
# HELP local_schedule_count The number of tasks scheduled from worker threads.
# TYPE local_schedule_count counter
local_schedule_count{measurement="min"} 0
local_schedule_count{measurement="max"} 0
local_schedule_count{measurement="total"} 0
# HELP overflow_count The number of times worker threads saturated their local queues.
# TYPE overflow_count counter
overflow_count{measurement="min"} 0
overflow_count{measurement="max"} 0
overflow_count{measurement="total"} 0
# HELP polls_count The number of tasks that have been polled across all worker threads.
# TYPE polls_count counter
polls_count{measurement="min"} 0
polls_count{measurement="max"} 0
polls_count{measurement="total"} 0
# HELP busy_duration_seconds The amount of time worker threads were busy.
# TYPE busy_duration_seconds counter
busy_duration_seconds{measurement="min"} 0.0
busy_duration_seconds{measurement="max"} 0.0
busy_duration_seconds{measurement="total"} 0.0
# HELP injection_queue_depth The number of tasks currently scheduled in the runtime's injection queue.
# TYPE injection_queue_depth gauge
injection_queue_depth 0
# HELP local_queue_depth The total number of tasks currently scheduled in workers' local queues.
# TYPE local_queue_depth counter
local_queue_depth{measurement="min"} 1
local_queue_depth{measurement="max"} 1
local_queue_depth{measurement="total"} 1
# HELP mean_polls_per_park The mean number of polls per park.
# TYPE mean_polls_per_park gauge
mean_polls_per_park 0.0
# HELP busy_ratio The proportion of time spent polling for tasks.
# TYPE busy_ratio gauge
busy_ratio NaN
# EOF
"#;
        assert_eq!(String::from_utf8(buffer).unwrap(), expected);
    }
}
