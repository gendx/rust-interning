use super::source;
use crate::intern::{IString, Interned, Interner, StringInterner};
use crate::size::EstimateSize;
use std::hash::Hash;
use std::ops::Deref;

#[derive(Default)]
pub struct Interners<'a> {
    string: StringInterner,
    disruption: Interner<Disruption<'a>>,
    line: Interner<Line<'a>>,
    application_period: Interner<ApplicationPeriod<'a>>,
    impacted_object: Interner<ImpactedObject<'a>>,
}

impl EstimateSize for Interners<'_> {
    fn allocated_bytes(&self) -> usize {
        self.string.allocated_bytes()
            + self.disruption.allocated_bytes()
            + self.line.allocated_bytes()
            + self.application_period.allocated_bytes()
            + self.impacted_object.allocated_bytes()
    }
}

impl Interners<'_> {
    pub fn print_summary(&self, total_bytes: usize) {
        self.string.print_summary("", "String", total_bytes);
        self.disruption.print_summary("", "Disruption", total_bytes);
        self.application_period
            .print_summary("  ", "ApplicationPeriod", total_bytes);
        self.line.print_summary("", "Line", total_bytes);
        self.impacted_object
            .print_summary("  ", "ImpactedObject", total_bytes);
    }
}

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

fn mixed_eq<T, U>(lhs: &Interned<'_, T>, rhs: &U) -> bool
where
    T: Eq + Hash + PartialEq<U>,
{
    lhs.lookup().deref() == rhs
}

fn slice_mixed_eq<T, U>(lhs: &[Interned<'_, T>], rhs: &[U]) -> bool
where
    T: Eq + Hash + PartialEq<U>,
{
    lhs.iter().eq_by(rhs.iter(), |t, u| mixed_eq(t, u))
}

fn option_vec_mixed_eq<T, U>(lhs: &Option<Vec<Interned<'_, T>>>, rhs: &Option<Vec<U>>) -> bool
where
    T: Eq + Hash + PartialEq<U>,
{
    match (lhs, rhs) {
        (None, None) => true,
        (None, Some(_)) | (Some(_), None) => false,
        (Some(x), Some(y)) => slice_mixed_eq(x, y),
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct Data<'a> {
    pub status_code: Option<i32>,
    pub error: Option<IString<'a>>,
    pub message: Option<IString<'a>>,
    pub disruptions: Option<Vec<Interned<'a, Disruption<'a>>>>,
    pub lines: Option<Vec<Interned<'a, Line<'a>>>>,
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
            && option_vec_mixed_eq(&self.disruptions, &other.disruptions)
            && option_vec_mixed_eq(&self.lines, &other.lines)
            && option_eq(&self.last_updated_date, &other.last_updated_date)
    }
}

impl<'a> Data<'a> {
    pub fn from(interners: &'a Interners<'a>, source: source::Data) -> Self {
        Self {
            status_code: source.status_code,
            error: source.error.map(|x| Interned::from(&interners.string, x)),
            message: source.message.map(|x| Interned::from(&interners.string, x)),
            disruptions: source.disruptions.map(|x| {
                x.into_iter()
                    .map(|x| Interned::from(&interners.disruption, Disruption::from(interners, x)))
                    .collect()
            }),
            lines: source.lines.map(|x| {
                x.into_iter()
                    .map(|x| Interned::from(&interners.line, Line::from(interners, x)))
                    .collect()
            }),
            last_updated_date: source
                .last_updated_date
                .map(|x| Interned::from(&interners.string, x)),
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct Disruption<'a> {
    pub id: IString<'a>,
    pub application_periods: Vec<Interned<'a, ApplicationPeriod<'a>>>,
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
            && slice_mixed_eq(&self.application_periods, &other.application_periods)
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
    pub fn from(interners: &'a Interners<'a>, source: source::Disruption) -> Self {
        Self {
            id: Interned::from(&interners.string, source.id),
            application_periods: source
                .application_periods
                .into_iter()
                .map(|x| {
                    Interned::from(
                        &interners.application_period,
                        ApplicationPeriod::from(interners, x),
                    )
                })
                .collect(),
            last_update: Interned::from(&interners.string, source.last_update),
            cause: Interned::from(&interners.string, source.cause),
            severity: Interned::from(&interners.string, source.severity),
            tags: source.tags.map(|x| {
                x.into_iter()
                    .map(|x| Interned::from(&interners.string, x))
                    .collect()
            }),
            title: Interned::from(&interners.string, source.title),
            message: Interned::from(&interners.string, source.message),
            disruption_id: source
                .disruption_id
                .map(|x| Interned::from(&interners.string, x)),
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
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
    pub fn from(interners: &'a Interners<'a>, source: source::ApplicationPeriod) -> Self {
        Self {
            begin: Interned::from(&interners.string, source.begin),
            end: Interned::from(&interners.string, source.end),
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct Line<'a> {
    pub id: IString<'a>,
    pub name: IString<'a>,
    pub short_name: IString<'a>,
    pub mode: IString<'a>,
    pub network_id: IString<'a>,
    pub impacted_objects: Vec<Interned<'a, ImpactedObject<'a>>>,
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
            && slice_mixed_eq(&self.impacted_objects, &other.impacted_objects)
    }
}

impl<'a> Line<'a> {
    pub fn from(interners: &'a Interners<'a>, source: source::Line) -> Self {
        Self {
            id: Interned::from(&interners.string, source.id),
            name: Interned::from(&interners.string, source.name),
            short_name: Interned::from(&interners.string, source.short_name),
            mode: Interned::from(&interners.string, source.mode),
            network_id: Interned::from(&interners.string, source.network_id),
            impacted_objects: source
                .impacted_objects
                .into_iter()
                .map(|x| {
                    Interned::from(
                        &interners.impacted_object,
                        ImpactedObject::from(interners, x),
                    )
                })
                .collect(),
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
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
    pub fn from(interners: &'a Interners<'a>, source: source::ImpactedObject) -> Self {
        Self {
            typ: Interned::from(&interners.string, source.typ),
            id: Interned::from(&interners.string, source.id),
            name: Interned::from(&interners.string, source.name),
            disruption_ids: source
                .disruption_ids
                .into_iter()
                .map(|x| Interned::from(&interners.string, x))
                .collect(),
        }
    }
}
