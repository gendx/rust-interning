use crate::size::EstimateSize;
use serde::Deserialize;
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Data {
    // Success case.
    pub disruptions: Option<Vec<Disruption>>,
    pub lines: Option<Vec<Line>>,
    #[serde(rename = "lastUpdatedDate")]
    pub last_updated_date: Option<String>,
    // Error case.
    #[serde(rename = "statusCode")]
    pub status_code: Option<i32>,
    pub error: Option<String>,
    pub message: Option<String>,
}

impl EstimateSize for Data {
    fn allocated_bytes(&self) -> usize {
        self.disruptions.allocated_bytes()
            + self.lines.allocated_bytes()
            + self.last_updated_date.allocated_bytes()
            + self.status_code.allocated_bytes()
            + self.error.allocated_bytes()
            + self.message.allocated_bytes()
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Disruption {
    pub id: Uuid,
    #[serde(rename = "applicationPeriods")]
    pub application_periods: Vec<ApplicationPeriod>,
    #[serde(rename = "lastUpdate")]
    pub last_update: String,
    pub cause: String,
    pub severity: String,
    pub tags: Option<Vec<String>>,
    pub title: String,
    pub message: Option<String>,
    #[serde(rename = "shortMessage")]
    pub short_message: Option<String>,
    pub disruption_id: Option<Uuid>,
}

impl EstimateSize for Disruption {
    fn allocated_bytes(&self) -> usize {
        self.id.allocated_bytes()
            + self.application_periods.allocated_bytes()
            + self.last_update.allocated_bytes()
            + self.cause.allocated_bytes()
            + self.severity.allocated_bytes()
            + self.tags.allocated_bytes()
            + self.title.allocated_bytes()
            + self.message.allocated_bytes()
            + self.short_message.allocated_bytes()
            + self.disruption_id.allocated_bytes()
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationPeriod {
    pub begin: String,
    pub end: String,
}

impl EstimateSize for ApplicationPeriod {
    fn allocated_bytes(&self) -> usize {
        self.begin.allocated_bytes() + self.end.allocated_bytes()
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Line {
    pub id: String,
    pub name: String,
    #[serde(rename = "shortName")]
    pub short_name: String,
    pub mode: String,
    #[serde(rename = "networkId")]
    pub network_id: String,
    #[serde(rename = "impactedObjects")]
    pub impacted_objects: Vec<ImpactedObject>,
}

impl EstimateSize for Line {
    fn allocated_bytes(&self) -> usize {
        self.id.allocated_bytes()
            + self.name.allocated_bytes()
            + self.short_name.allocated_bytes()
            + self.mode.allocated_bytes()
            + self.network_id.allocated_bytes()
            + self.impacted_objects.allocated_bytes()
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImpactedObject {
    #[serde(rename = "type")]
    pub typ: String,
    pub id: String,
    pub name: String,
    #[serde(rename = "disruptionIds")]
    pub disruption_ids: Vec<Uuid>,
}

impl EstimateSize for ImpactedObject {
    fn allocated_bytes(&self) -> usize {
        self.typ.allocated_bytes()
            + self.id.allocated_bytes()
            + self.name.allocated_bytes()
            + self.disruption_ids.allocated_bytes()
    }
}
