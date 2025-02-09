use super::source;
use crate::intern::{EqWith, IString, Interned, Interner, StringInterner};
use crate::size::EstimateSize;
use chrono::format::SecondsFormat;
use chrono::offset::LocalResult;
use chrono::{DateTime, NaiveDateTime};
use chrono_tz::Europe::Paris;
use serde::{Deserialize, Serialize};
use std::hash::Hash;
use uuid::Uuid;

#[derive(Default, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Interners {
    string: StringInterner,
    uuid: Interner<Uuid>,
    disruption_set: Interner<InternedSet<Disruption>>,
    disruption: Interner<Disruption>,
    application_period: Interner<ApplicationPeriod>,
    line_set: Interner<InternedSet<Line>>,
    line: Interner<Line>,
    line_header: Interner<LineHeader>,
    impacted_object: Interner<ImpactedObject>,
    object: Interner<Object>,
    uuid_set: Interner<InternedSet<Uuid>>,
}

impl EstimateSize for Interners {
    fn allocated_bytes(&self) -> usize {
        self.string.allocated_bytes()
            + self.uuid.allocated_bytes()
            + self.disruption_set.allocated_bytes()
            + self.disruption.allocated_bytes()
            + self.application_period.allocated_bytes()
            + self.line_set.allocated_bytes()
            + self.line.allocated_bytes()
            + self.line_header.allocated_bytes()
            + self.impacted_object.allocated_bytes()
            + self.object.allocated_bytes()
            + self.uuid_set.allocated_bytes()
    }
}

impl Interners {
    pub fn print_summary(&self, total_bytes: usize) {
        self.string.print_summary("", "String", total_bytes);
        self.uuid.print_summary("", "Uuid", total_bytes);
        self.disruption_set
            .print_summary("", "InternedSet<Disruption>", total_bytes);
        self.disruption
            .print_summary("  ", "Disruption", total_bytes);
        self.application_period
            .print_summary("    ", "ApplicationPeriod", total_bytes);
        self.line_set
            .print_summary("", "InternedSet<Line>", total_bytes);
        self.line.print_summary("  ", "Line", total_bytes);
        self.line_header
            .print_summary("    ", "LineHeader", total_bytes);
        self.impacted_object
            .print_summary("    ", "ImpactedObject", total_bytes);
        self.object.print_summary("      ", "Object", total_bytes);
        self.uuid_set
            .print_summary("      ", "InternedSet<Uuid>", total_bytes);
    }
}

fn option_eq_by<T, U>(lhs: &Option<T>, rhs: &Option<U>, pred: impl Fn(&T, &U) -> bool) -> bool {
    match (lhs, rhs) {
        (None, None) => true,
        (None, Some(_)) | (Some(_), None) => false,
        (Some(x), Some(y)) => pred(x, y),
    }
}

fn set_eq_by<T, U>(lhs: &[T], rhs: &[U], pred: impl Fn(&T, &U) -> bool) -> bool {
    let len = lhs.len();
    if len != rhs.len() {
        return false;
    }

    let mut used = vec![false; len];
    'outer: for x in lhs.iter() {
        for (i, y) in rhs.iter().enumerate() {
            if used[i] {
                continue;
            }
            if pred(x, y) {
                used[i] = true;
                continue 'outer;
            }
        }
        return false;
    }

    true
}

#[derive(Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct InternedSet<T> {
    set: Box<[Interned<T>]>,
}

impl<T> EstimateSize for InternedSet<T> {
    fn allocated_bytes(&self) -> usize {
        self.set.allocated_bytes()
    }
}

impl<T> InternedSet<T> {
    fn new(set: impl IntoIterator<Item = Interned<T>>) -> Self {
        let mut set: Box<[_]> = set.into_iter().collect();
        set.sort_unstable();
        Self { set }
    }

    fn set_eq_by<U>(&self, rhs: &[U], pred: impl Fn(&Interned<T>, &U) -> bool) -> bool {
        set_eq_by(&self.set, rhs, pred)
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimestampSecondsParis(i64);

impl EstimateSize for TimestampSecondsParis {
    fn allocated_bytes(&self) -> usize {
        0
    }
}

impl TimestampSecondsParis {
    fn from_formatted(x: &str, format: &str) -> Self {
        let naive_datetime = NaiveDateTime::parse_from_str(x, format).unwrap_or_else(|_| {
            panic!("Failed to parse datetime (custom format {format:?}) from {x}")
        });
        let datetime = match naive_datetime.and_local_timezone(Paris) {
            LocalResult::Single(x) => x,
            LocalResult::Ambiguous(earliest, latest) => {
                eprintln!("Ambiguous mapping of {naive_datetime:?} to the Paris timezone: {earliest:?} or {latest:?}");
                earliest
            }
            LocalResult::None => {
                panic!("Invalid mapping of {naive_datetime:?} to the Paris timezone")
            }
        };
        TimestampSecondsParis(datetime.timestamp())
    }

    fn to_formatted(&self, format: &str) -> String {
        DateTime::from_timestamp(self.0, 0)
            .unwrap()
            .with_timezone(&Paris)
            .naive_local()
            .format(format)
            .to_string()
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimestampMillis(i64);

impl EstimateSize for TimestampMillis {
    fn allocated_bytes(&self) -> usize {
        0
    }
}

impl TimestampMillis {
    fn from_rfc3339(x: &str) -> Self {
        let datetime = DateTime::parse_from_rfc3339(x)
            .unwrap_or_else(|_| panic!("Failed to parse datetime (RFC 3339 format) from {x}"));
        TimestampMillis(datetime.timestamp_millis())
    }

    fn to_rfc3339(&self) -> String {
        DateTime::from_timestamp_millis(self.0)
            .unwrap()
            .to_rfc3339_opts(SecondsFormat::Millis, true)
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum Data {
    Success {
        disruptions: Interned<InternedSet<Disruption>>,
        lines: Interned<InternedSet<Line>>,
        last_updated_date: TimestampMillis,
    },
    Error {
        status_code: i32,
        error: IString,
        message: IString,
    },
}

impl EstimateSize for Data {
    fn allocated_bytes(&self) -> usize {
        match self {
            Data::Success {
                disruptions,
                lines,
                last_updated_date,
            } => {
                disruptions.allocated_bytes()
                    + lines.allocated_bytes()
                    + last_updated_date.allocated_bytes()
            }
            Data::Error {
                status_code,
                error,
                message,
            } => {
                status_code.allocated_bytes() + error.allocated_bytes() + message.allocated_bytes()
            }
        }
    }
}

impl EqWith<source::Data, Interners> for Data {
    fn eq_with(&self, other: &source::Data, interners: &Interners) -> bool {
        match self {
            Data::Success {
                disruptions,
                lines,
                last_updated_date,
            } => {
                other.disruptions.as_ref().is_some_and(|other| {
                    disruptions
                        .lookup(&interners.disruption_set)
                        .set_eq_by(other, |x, y| {
                            x.eq_with_more(y, &interners.disruption, interners)
                        })
                }) && other.lines.as_ref().is_some_and(|other| {
                    lines
                        .lookup(&interners.line_set)
                        .set_eq_by(other, |x, y| x.eq_with_more(y, &interners.line, interners))
                }) && other
                    .last_updated_date
                    .as_ref()
                    .is_some_and(|other| last_updated_date.to_rfc3339() == *other)
                    && other.status_code.is_none()
                    && other.error.is_none()
                    && other.message.is_none()
            }
            Data::Error {
                status_code,
                error,
                message,
            } => {
                other
                    .status_code
                    .as_ref()
                    .is_some_and(|other| status_code == other)
                    && other
                        .error
                        .as_ref()
                        .is_some_and(|other| error.eq_with(other, &interners.string))
                    && other
                        .message
                        .as_ref()
                        .is_some_and(|other| message.eq_with(other, &interners.string))
                    && other.disruptions.is_none()
                    && other.lines.is_none()
                    && other.last_updated_date.is_none()
            }
        }
    }
}

impl Data {
    pub fn from(interners: &mut Interners, source: source::Data) -> Self {
        match source {
            source::Data {
                disruptions: Some(disruptions),
                lines: Some(lines),
                last_updated_date: Some(last_updated_date),
                status_code: None,
                error: None,
                message: None,
            } => {
                let disruptions = InternedSet::new(disruptions.into_iter().map(|x| {
                    let disruption = Disruption::from(interners, x);
                    Interned::from(&mut interners.disruption, disruption)
                }));
                let lines = InternedSet::new(lines.into_iter().map(|x| {
                    let line = Line::from(interners, x);
                    Interned::from(&mut interners.line, line)
                }));
                Data::Success {
                    disruptions: Interned::from(&mut interners.disruption_set, disruptions),
                    lines: Interned::from(&mut interners.line_set, lines),
                    last_updated_date: TimestampMillis::from_rfc3339(&last_updated_date),
                }
            }
            source::Data {
                disruptions: None,
                lines: None,
                last_updated_date: None,
                status_code: Some(status_code),
                error: Some(error),
                message: Some(message),
            } => Data::Error {
                status_code,
                error: Interned::from(&mut interners.string, error),
                message: Interned::from(&mut interners.string, message),
            },
            _ => panic!("Invalid data: {source:?}"),
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct Disruption {
    pub id: Interned<Uuid>,
    pub application_periods: InternedSet<ApplicationPeriod>,
    pub last_update: TimestampSecondsParis,
    pub cause: IString,
    pub severity: IString,
    pub tags: Option<InternedSet<String>>,
    pub title: IString,
    pub message: IString,
    pub disruption_id: Option<Interned<Uuid>>,
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
            + self.disruption_id.allocated_bytes()
    }
}

impl EqWith<source::Disruption, Interners> for Disruption {
    fn eq_with(&self, other: &source::Disruption, interners: &Interners) -> bool {
        self.id.eq_with(&other.id, &interners.uuid)
            && self
                .application_periods
                .set_eq_by(&other.application_periods, |x, y| {
                    x.eq_with_more(y, &interners.application_period, interners)
                })
            && self.last_update.to_formatted("%Y%m%dT%H%M%S") == other.last_update
            && self.cause.eq_with(&other.cause, &interners.string)
            && self.severity.eq_with(&other.severity, &interners.string)
            && option_eq_by(&self.tags, &other.tags, |x, y| {
                x.set_eq_by(y, |x, y| x.eq_with(y, &interners.string))
            })
            && self.title.eq_with(&other.title, &interners.string)
            && self.message.eq_with(&other.message, &interners.string)
            && option_eq_by(&self.disruption_id, &other.disruption_id, |x, y| {
                x.eq_with(y, &interners.uuid)
            })
    }
}

impl Disruption {
    pub fn from(interners: &mut Interners, source: source::Disruption) -> Self {
        Self {
            id: Interned::from(&mut interners.uuid, source.id),
            application_periods: InternedSet::new(source.application_periods.into_iter().map(
                |x| {
                    let application_period = ApplicationPeriod::from(interners, x);
                    Interned::from(&mut interners.application_period, application_period)
                },
            )),
            last_update: TimestampSecondsParis::from_formatted(
                &source.last_update,
                "%Y%m%dT%H%M%S",
            ),
            cause: Interned::from(&mut interners.string, source.cause),
            severity: Interned::from(&mut interners.string, source.severity),
            tags: source.tags.map(|x| {
                InternedSet::new(
                    x.into_iter()
                        .map(|x| Interned::from(&mut interners.string, x)),
                )
            }),
            title: Interned::from(&mut interners.string, source.title),
            message: Interned::from(&mut interners.string, source.message),
            disruption_id: source
                .disruption_id
                .map(|x| Interned::from(&mut interners.uuid, x)),
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplicationPeriod {
    pub begin: TimestampSecondsParis,
    pub end: TimestampSecondsParis,
}

impl EstimateSize for ApplicationPeriod {
    fn allocated_bytes(&self) -> usize {
        self.begin.allocated_bytes() + self.end.allocated_bytes()
    }
}

impl EqWith<source::ApplicationPeriod, Interners> for ApplicationPeriod {
    fn eq_with(&self, other: &source::ApplicationPeriod, _interners: &Interners) -> bool {
        self.begin.to_formatted("%Y%m%dT%H%M%S") == other.begin
            && self.end.to_formatted("%Y%m%dT%H%M%S") == other.end
    }
}

impl ApplicationPeriod {
    pub fn from(_interners: &mut Interners, source: source::ApplicationPeriod) -> Self {
        Self {
            begin: TimestampSecondsParis::from_formatted(&source.begin, "%Y%m%dT%H%M%S"),
            end: TimestampSecondsParis::from_formatted(&source.end, "%Y%m%dT%H%M%S"),
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct Line {
    pub header: Interned<LineHeader>,
    pub impacted_objects: InternedSet<ImpactedObject>,
}

impl EstimateSize for Line {
    fn allocated_bytes(&self) -> usize {
        self.header.allocated_bytes() + self.impacted_objects.allocated_bytes()
    }
}

impl EqWith<source::Line, Interners> for Line {
    fn eq_with(&self, other: &source::Line, interners: &Interners) -> bool {
        self.header
            .eq_with_more(other, &interners.line_header, interners)
            && self
                .impacted_objects
                .set_eq_by(&other.impacted_objects, |x, y| {
                    x.eq_with_more(y, &interners.impacted_object, interners)
                })
    }
}

impl Line {
    pub fn from(interners: &mut Interners, source: source::Line) -> Self {
        Self {
            header: Interned::from(
                &mut interners.line_header,
                LineHeader {
                    id: Interned::from(&mut interners.string, source.id),
                    name: Interned::from(&mut interners.string, source.name),
                    short_name: Interned::from(&mut interners.string, source.short_name),
                    mode: Interned::from(&mut interners.string, source.mode),
                    network_id: Interned::from(&mut interners.string, source.network_id),
                },
            ),
            impacted_objects: InternedSet::new(source.impacted_objects.into_iter().map(|x| {
                let impacted_object = ImpactedObject::from(interners, x);
                Interned::from(&mut interners.impacted_object, impacted_object)
            })),
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct LineHeader {
    pub id: IString,
    pub name: IString,
    pub short_name: IString,
    pub mode: IString,
    pub network_id: IString,
}

impl EstimateSize for LineHeader {
    fn allocated_bytes(&self) -> usize {
        self.id.allocated_bytes()
            + self.name.allocated_bytes()
            + self.short_name.allocated_bytes()
            + self.mode.allocated_bytes()
            + self.network_id.allocated_bytes()
    }
}

impl EqWith<source::Line, Interners> for LineHeader {
    fn eq_with(&self, other: &source::Line, interners: &Interners) -> bool {
        self.id.eq_with(&other.id, &interners.string)
            && self.name.eq_with(&other.name, &interners.string)
            && self
                .short_name
                .eq_with(&other.short_name, &interners.string)
            && self.mode.eq_with(&other.mode, &interners.string)
            && self
                .network_id
                .eq_with(&other.network_id, &interners.string)
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImpactedObject {
    pub object: Interned<Object>,
    pub disruption_ids: Interned<InternedSet<Uuid>>,
}

impl EstimateSize for ImpactedObject {
    fn allocated_bytes(&self) -> usize {
        self.object.allocated_bytes() + self.disruption_ids.allocated_bytes()
    }
}

impl EqWith<source::ImpactedObject, Interners> for ImpactedObject {
    fn eq_with(&self, other: &source::ImpactedObject, interners: &Interners) -> bool {
        self.object
            .eq_with_more(other, &interners.object, interners)
            && self
                .disruption_ids
                .lookup(&interners.uuid_set)
                .set_eq_by(&other.disruption_ids, |x, y| x.eq_with(y, &interners.uuid))
    }
}

impl ImpactedObject {
    pub fn from(interners: &mut Interners, source: source::ImpactedObject) -> Self {
        let disruption_ids = InternedSet::new(
            source
                .disruption_ids
                .into_iter()
                .map(|x| Interned::from(&mut interners.uuid, x)),
        );
        Self {
            object: Interned::from(
                &mut interners.object,
                Object {
                    typ: Interned::from(&mut interners.string, source.typ),
                    id: Interned::from(&mut interners.string, source.id),
                    name: Interned::from(&mut interners.string, source.name),
                },
            ),
            disruption_ids: Interned::from(&mut interners.uuid_set, disruption_ids),
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct Object {
    pub typ: IString,
    pub id: IString,
    pub name: IString,
}

impl EstimateSize for Object {
    fn allocated_bytes(&self) -> usize {
        self.typ.allocated_bytes() + self.id.allocated_bytes() + self.name.allocated_bytes()
    }
}

impl EqWith<source::ImpactedObject, Interners> for Object {
    fn eq_with(&self, other: &source::ImpactedObject, interners: &Interners) -> bool {
        self.typ.eq_with(&other.typ, &interners.string)
            && self.id.eq_with(&other.id, &interners.string)
            && self.name.eq_with(&other.name, &interners.string)
    }
}
