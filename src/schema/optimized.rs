use super::source;
use crate::intern::{IString, StringInterner};
use crate::size::EstimateSize;

// Somehow `Option<T>` doesn't implement `PartialEq<Option<U>>` where
// `T: PartialEq<U>` so we implement it by hand.
fn option_eq<T, U>(lhs: &Option<T>, rhs: &Option<U>) -> bool
where
    T: PartialEq<U>,
{
    match (lhs, rhs) {
        (None, None) => true,
        (None, Some(_)) | (Some(_), None) => false,
        (Some(x), Some(y)) => x == y,
    }
}

#[derive(Debug)]
pub struct Data<'a> {
    pub status_code: Option<i32>,
    pub error: Option<IString<'a>>,
    pub message: Option<IString<'a>>,
    pub disruptions: Option<Vec<Disruption<'a>>>,
    pub lines: Option<Vec<Line<'a>>>,
    pub last_updated_date: Option<IString<'a>>,
}

impl EstimateSize for Data<'_> {
    fn allocated_bytes(&self) -> usize {
        self.status_code.allocated_bytes()
            + self.error.allocated_bytes()
            + self.message.allocated_bytes()
            + self.disruptions.allocated_bytes()
            + self.lines.allocated_bytes()
            + self.last_updated_date.allocated_bytes()
    }
}

impl PartialEq<source::Data> for Data<'_> {
    fn eq(&self, other: &source::Data) -> bool {
        self.status_code == other.status_code
            && option_eq(&self.error, &other.error)
            && option_eq(&self.message, &other.message)
            && option_eq(&self.disruptions, &other.disruptions)
            && option_eq(&self.lines, &other.lines)
            && option_eq(&self.last_updated_date, &other.last_updated_date)
    }
}

impl<'a> Data<'a> {
    pub fn from(interner: &'a StringInterner, source: source::Data) -> Self {
        Self {
            status_code: source.status_code,
            error: source.error.map(|x| IString::from(interner, x)),
            message: source.message.map(|x| IString::from(interner, x)),
            disruptions: source.disruptions.map(|x| {
                x.into_iter()
                    .map(|x| Disruption::from(interner, x))
                    .collect()
            }),
            lines: source
                .lines
                .map(|x| x.into_iter().map(|x| Line::from(interner, x)).collect()),
            last_updated_date: source.last_updated_date.map(|x| IString::from(interner, x)),
        }
    }
}

#[derive(Debug)]
pub struct Disruption<'a> {
    pub id: IString<'a>,
    pub application_periods: Vec<ApplicationPeriod<'a>>,
    pub last_update: IString<'a>,
    pub cause: IString<'a>,
    pub severity: IString<'a>,
    pub tags: Option<Vec<IString<'a>>>,
    pub title: IString<'a>,
    pub message: IString<'a>,
    pub disruption_id: Option<IString<'a>>,
}

impl EstimateSize for Disruption<'_> {
    fn allocated_bytes(&self) -> usize {
        self.id.allocated_bytes()
            + self.application_periods.allocated_bytes()
            + self.last_update.allocated_bytes()
            + self.cause.allocated_bytes()
            + self.severity.allocated_bytes()
            + self.tags.allocated_bytes()
            + self.title.allocated_bytes()
            + self.message.allocated_bytes()
            + self.disruption_id.allocated_bytes()
    }
}

impl PartialEq<source::Disruption> for Disruption<'_> {
    fn eq(&self, other: &source::Disruption) -> bool {
        self.id == other.id
            && self.application_periods == other.application_periods
            && self.last_update == other.last_update
            && self.cause == other.cause
            && self.severity == other.severity
            && option_eq(&self.tags, &other.tags)
            && self.title == other.title
            && self.message == other.message
            && option_eq(&self.disruption_id, &other.disruption_id)
    }
}

impl<'a> Disruption<'a> {
    pub fn from(interner: &'a StringInterner, source: source::Disruption) -> Self {
        Self {
            id: IString::from(interner, source.id),
            application_periods: source
                .application_periods
                .into_iter()
                .map(|x| ApplicationPeriod::from(interner, x))
                .collect(),
            last_update: IString::from(interner, source.last_update),
            cause: IString::from(interner, source.cause),
            severity: IString::from(interner, source.severity),
            tags: source
                .tags
                .map(|x| x.into_iter().map(|x| IString::from(interner, x)).collect()),
            title: IString::from(interner, source.title),
            message: IString::from(interner, source.message),
            disruption_id: source.disruption_id.map(|x| IString::from(interner, x)),
        }
    }
}

#[derive(Debug)]
pub struct ApplicationPeriod<'a> {
    pub begin: IString<'a>,
    pub end: IString<'a>,
}

impl EstimateSize for ApplicationPeriod<'_> {
    fn allocated_bytes(&self) -> usize {
        self.begin.allocated_bytes() + self.end.allocated_bytes()
    }
}

impl PartialEq<source::ApplicationPeriod> for ApplicationPeriod<'_> {
    fn eq(&self, other: &source::ApplicationPeriod) -> bool {
        self.begin == other.begin && self.end == other.end
    }
}

impl<'a> ApplicationPeriod<'a> {
    pub fn from(interner: &'a StringInterner, source: source::ApplicationPeriod) -> Self {
        Self {
            begin: IString::from(interner, source.begin),
            end: IString::from(interner, source.end),
        }
    }
}

#[derive(Debug)]
pub struct Line<'a> {
    pub id: IString<'a>,
    pub name: IString<'a>,
    pub short_name: IString<'a>,
    pub mode: IString<'a>,
    pub network_id: IString<'a>,
    pub impacted_objects: Vec<ImpactedObject<'a>>,
}

impl EstimateSize for Line<'_> {
    fn allocated_bytes(&self) -> usize {
        self.id.allocated_bytes()
            + self.name.allocated_bytes()
            + self.short_name.allocated_bytes()
            + self.mode.allocated_bytes()
            + self.network_id.allocated_bytes()
            + self.impacted_objects.allocated_bytes()
    }
}

impl PartialEq<source::Line> for Line<'_> {
    fn eq(&self, other: &source::Line) -> bool {
        self.id == other.id
            && self.name == other.name
            && self.short_name == other.short_name
            && self.mode == other.mode
            && self.network_id == other.network_id
            && self.impacted_objects == other.impacted_objects
    }
}

impl<'a> Line<'a> {
    pub fn from(interner: &'a StringInterner, source: source::Line) -> Self {
        Self {
            id: IString::from(interner, source.id),
            name: IString::from(interner, source.name),
            short_name: IString::from(interner, source.short_name),
            mode: IString::from(interner, source.mode),
            network_id: IString::from(interner, source.network_id),
            impacted_objects: source
                .impacted_objects
                .into_iter()
                .map(|x| ImpactedObject::from(interner, x))
                .collect(),
        }
    }
}

#[derive(Debug)]
pub struct ImpactedObject<'a> {
    pub typ: IString<'a>,
    pub id: IString<'a>,
    pub name: IString<'a>,
    pub disruption_ids: Vec<IString<'a>>,
}

impl EstimateSize for ImpactedObject<'_> {
    fn allocated_bytes(&self) -> usize {
        self.typ.allocated_bytes()
            + self.id.allocated_bytes()
            + self.name.allocated_bytes()
            + self.disruption_ids.allocated_bytes()
    }
}

impl PartialEq<source::ImpactedObject> for ImpactedObject<'_> {
    fn eq(&self, other: &source::ImpactedObject) -> bool {
        self.typ == other.typ
            && self.id == other.id
            && self.name == other.name
            && self.disruption_ids == other.disruption_ids
    }
}

impl<'a> ImpactedObject<'a> {
    pub fn from(interner: &'a StringInterner, source: source::ImpactedObject) -> Self {
        Self {
            typ: IString::from(interner, source.typ),
            id: IString::from(interner, source.id),
            name: IString::from(interner, source.name),
            disruption_ids: source
                .disruption_ids
                .into_iter()
                .map(|x| IString::from(interner, x))
                .collect(),
        }
    }
}
