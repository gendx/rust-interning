use super::Uuid;
use get_size2::GetSize;
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize, GetSize)]
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

#[derive(Clone, Debug, Deserialize, GetSize)]
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

#[derive(Clone, Debug, Deserialize, GetSize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationPeriod {
    pub begin: String,
    pub end: String,
}

#[derive(Clone, Debug, Deserialize, GetSize)]
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

#[derive(Clone, Debug, Deserialize, GetSize)]
#[serde(deny_unknown_fields)]
pub struct ImpactedObject {
    #[serde(rename = "type")]
    pub typ: String,
    pub id: String,
    pub name: String,
    #[serde(rename = "disruptionIds")]
    pub disruption_ids: Vec<Uuid>,
}
