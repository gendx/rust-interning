use super::source;
use super::Uuid;
use crate::compare::EqWith;
use blazinterner::{Arena, Interned};
use chrono::format::SecondsFormat;
use chrono::offset::LocalResult;
use chrono::{DateTime, NaiveDateTime};
use chrono_tz::Europe::Paris;
use get_size2::{GetSize, GetSizeTracker};
use serde::de::{SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_tuple::{Deserialize_tuple, Serialize_tuple};
use std::hash::Hash;
use std::marker::PhantomData;

type StringArena = Arena<str, Box<str>>;
type IString = Interned<str, Box<str>>;

#[derive(Default, Debug, PartialEq, Eq, Serialize_tuple, Deserialize_tuple, GetSize)]
pub struct Arenas {
    string: StringArena,
    uuid: Arena<Uuid>,
    disruption_set: Arena<InternedSet<Disruption>>,
    disruption: Arena<Disruption>,
    application_period: Arena<ApplicationPeriod>,
    line_set: Arena<InternedSet<Line>>,
    line: Arena<Line>,
    line_header: Arena<LineHeader>,
    impacted_object: Arena<ImpactedObject>,
    object: Arena<Object>,
    uuid_set: Arena<InternedSet<Uuid>>,
}

impl Arenas {
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

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct InternedSet<T: ?Sized, Storage = T> {
    set: Box<[Interned<T, Storage>]>,
}

impl<T: ?Sized, Storage> GetSize for InternedSet<T, Storage> {
    fn get_heap_size_with_tracker<Tr: GetSizeTracker>(&self, tracker: Tr) -> (usize, Tr) {
        self.set.get_heap_size_with_tracker(tracker)
    }
}

impl<T: ?Sized, Storage> InternedSet<T, Storage> {
    fn new(set: impl IntoIterator<Item = Interned<T, Storage>>) -> Self {
        let mut set: Box<[_]> = set.into_iter().collect();
        set.sort_unstable();
        Self { set }
    }

    fn set_eq_by<U>(&self, rhs: &[U], pred: impl Fn(&Interned<T, Storage>, &U) -> bool) -> bool {
        set_eq_by(&self.set, rhs, pred)
    }
}

impl<T: ?Sized, Storage> Serialize for InternedSet<T, Storage> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut rle_encoded = Vec::with_capacity(self.set.len());
        let mut prev: Option<u32> = None;
        let mut streak: i32 = 0;

        for x in &self.set {
            let id = x.id();
            let diff = id - prev.unwrap_or(0);
            if prev.is_some() && diff == 1 {
                streak += 1;
            } else {
                if streak != 0 {
                    rle_encoded.push(-streak);
                    streak = 0;
                }
                rle_encoded.push(diff as i32);
            }
            prev = Some(id);
        }
        if streak != 0 {
            rle_encoded.push(-streak);
        }

        serializer.collect_seq(rle_encoded)
    }
}

impl<'de, T: ?Sized, Storage> Deserialize<'de> for InternedSet<T, Storage> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(InternedSetVisitor::new())
    }
}

struct InternedSetVisitor<T: ?Sized, Storage> {
    _phantom: PhantomData<fn() -> InternedSet<T, Storage>>,
}

impl<T: ?Sized, Storage> InternedSetVisitor<T, Storage> {
    fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<'de, T: ?Sized, Storage> Visitor<'de> for InternedSetVisitor<T, Storage> {
    type Value = InternedSet<T, Storage>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a sequence of values")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut set = match seq.size_hint() {
            None => Vec::new(),
            Some(size_hint) => Vec::with_capacity(size_hint),
        };

        let mut prev = 0;
        while let Some(x) = seq.next_element::<i32>()? {
            if x < 0 {
                for _ in 0..-x {
                    prev += 1;
                    set.push(Interned::from_id(prev));
                }
            } else {
                prev += x as u32;
                set.push(Interned::from_id(prev));
            }
        }

        Ok(InternedSet {
            set: set.into_boxed_slice(),
        })
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Serialize, Deserialize, GetSize)]
pub struct TimestampSecondsParis(i64);

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

#[derive(Debug, Hash, PartialEq, Eq, Serialize, Deserialize, GetSize)]
pub struct TimestampMillis(i64);

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

#[derive(Debug, Hash, PartialEq, Eq, Serialize, Deserialize, GetSize)]
pub enum Data {
    Success(DataSuccess),
    Error(DataError),
}

impl EqWith<source::Data, Arenas> for Data {
    fn eq_with(&self, other: &source::Data, arenas: &Arenas) -> bool {
        match self {
            Data::Success(data) => data.eq_with(other, arenas),
            Data::Error(data) => data.eq_with(other, arenas),
        }
    }
}

impl Data {
    pub fn from(arenas: &Arenas, source: source::Data) -> Self {
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
                    let disruption = Disruption::from(arenas, x);
                    Interned::from(&arenas.disruption, disruption)
                }));
                let lines = InternedSet::new(lines.into_iter().map(|x| {
                    let line = Line::from(arenas, x);
                    Interned::from(&arenas.line, line)
                }));
                Data::Success(DataSuccess {
                    disruptions: Interned::from(&arenas.disruption_set, disruptions),
                    lines: Interned::from(&arenas.line_set, lines),
                    last_updated_date: TimestampMillis::from_rfc3339(&last_updated_date),
                })
            }
            source::Data {
                disruptions: None,
                lines: None,
                last_updated_date: None,
                status_code: Some(status_code),
                error: Some(error),
                message: Some(message),
            } => Data::Error(DataError {
                status_code,
                error: Interned::from(&arenas.string, error),
                message: Interned::from(&arenas.string, message),
            }),
            _ => panic!("Invalid data: {source:?}"),
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Serialize_tuple, Deserialize_tuple, GetSize)]
pub struct DataSuccess {
    disruptions: Interned<InternedSet<Disruption>>,
    lines: Interned<InternedSet<Line>>,
    last_updated_date: TimestampMillis,
}

impl EqWith<source::Data, Arenas> for DataSuccess {
    fn eq_with(&self, other: &source::Data, arenas: &Arenas) -> bool {
        other.disruptions.as_ref().is_some_and(|other| {
            self.disruptions
                .lookup_ref(&arenas.disruption_set)
                .set_eq_by(other, |x, y| {
                    x.lookup_ref(&arenas.disruption).eq_with(y, arenas)
                })
        }) && other.lines.as_ref().is_some_and(|other| {
            self.lines
                .lookup_ref(&arenas.line_set)
                .set_eq_by(other, |x, y| x.lookup_ref(&arenas.line).eq_with(y, arenas))
        }) && other
            .last_updated_date
            .as_ref()
            .is_some_and(|other| self.last_updated_date.to_rfc3339() == *other)
            && other.status_code.is_none()
            && other.error.is_none()
            && other.message.is_none()
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Serialize_tuple, Deserialize_tuple, GetSize)]
pub struct DataError {
    status_code: i32,
    error: IString,
    message: IString,
}

impl EqWith<source::Data, Arenas> for DataError {
    fn eq_with(&self, other: &source::Data, arenas: &Arenas) -> bool {
        other
            .status_code
            .as_ref()
            .is_some_and(|other| self.status_code == *other)
            && other
                .error
                .as_ref()
                .is_some_and(|other| self.error.eq_with(other, &arenas.string))
            && other
                .message
                .as_ref()
                .is_some_and(|other| self.message.eq_with(other, &arenas.string))
            && other.disruptions.is_none()
            && other.lines.is_none()
            && other.last_updated_date.is_none()
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Serialize_tuple, Deserialize_tuple, GetSize)]
pub struct Disruption {
    pub id: Interned<Uuid>,
    pub application_periods: InternedSet<ApplicationPeriod>,
    pub last_update: TimestampSecondsParis,
    pub cause: IString,
    pub severity: IString,
    pub tags: Option<InternedSet<str, Box<str>>>,
    pub title: IString,
    pub message: Option<IString>,
    pub short_message: Option<IString>,
    pub disruption_id: Option<Interned<Uuid>>,
}

impl EqWith<source::Disruption, Arenas> for Disruption {
    fn eq_with(&self, other: &source::Disruption, arenas: &Arenas) -> bool {
        self.id.eq_with(&other.id, &arenas.uuid)
            && self
                .application_periods
                .set_eq_by(&other.application_periods, |x, y| {
                    x.lookup_ref(&arenas.application_period).eq_with(y, arenas)
                })
            && self.last_update.to_formatted("%Y%m%dT%H%M%S") == other.last_update
            && self.cause.eq_with(&other.cause, &arenas.string)
            && self.severity.eq_with(&other.severity, &arenas.string)
            && option_eq_by(&self.tags, &other.tags, |x, y| {
                x.set_eq_by(y, |x, y| x.eq_with(y, &arenas.string))
            })
            && self.title.eq_with(&other.title, &arenas.string)
            && option_eq_by(&self.message, &other.message, |x, y| {
                x.eq_with(y, &arenas.string)
            })
            && option_eq_by(&self.short_message, &other.short_message, |x, y| {
                x.eq_with(y, &arenas.string)
            })
            && option_eq_by(&self.disruption_id, &other.disruption_id, |x, y| {
                x.eq_with(y, &arenas.uuid)
            })
    }
}

impl Disruption {
    pub fn from(arenas: &Arenas, source: source::Disruption) -> Self {
        Self {
            id: Interned::from(&arenas.uuid, source.id),
            application_periods: InternedSet::new(source.application_periods.into_iter().map(
                |x| {
                    let application_period = ApplicationPeriod::from(arenas, x);
                    Interned::from(&arenas.application_period, application_period)
                },
            )),
            last_update: TimestampSecondsParis::from_formatted(
                &source.last_update,
                "%Y%m%dT%H%M%S",
            ),
            cause: Interned::from(&arenas.string, source.cause),
            severity: Interned::from(&arenas.string, source.severity),
            tags: source.tags.map(|x| {
                InternedSet::new(x.into_iter().map(|x| Interned::from(&arenas.string, x)))
            }),
            title: Interned::from(&arenas.string, source.title),
            message: source.message.map(|x| Interned::from(&arenas.string, x)),
            short_message: source
                .short_message
                .map(|x| Interned::from(&arenas.string, x)),
            disruption_id: source
                .disruption_id
                .map(|x| Interned::from(&arenas.uuid, x)),
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Serialize_tuple, Deserialize_tuple, GetSize)]
pub struct ApplicationPeriod {
    pub begin: TimestampSecondsParis,
    pub end: TimestampSecondsParis,
}

impl EqWith<source::ApplicationPeriod, Arenas> for ApplicationPeriod {
    fn eq_with(&self, other: &source::ApplicationPeriod, _arenas: &Arenas) -> bool {
        self.begin.to_formatted("%Y%m%dT%H%M%S") == other.begin
            && self.end.to_formatted("%Y%m%dT%H%M%S") == other.end
    }
}

impl ApplicationPeriod {
    pub fn from(_arenas: &Arenas, source: source::ApplicationPeriod) -> Self {
        Self {
            begin: TimestampSecondsParis::from_formatted(&source.begin, "%Y%m%dT%H%M%S"),
            end: TimestampSecondsParis::from_formatted(&source.end, "%Y%m%dT%H%M%S"),
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Serialize_tuple, Deserialize_tuple, GetSize)]
pub struct Line {
    pub header: Interned<LineHeader>,
    pub impacted_objects: InternedSet<ImpactedObject>,
}

impl EqWith<source::Line, Arenas> for Line {
    fn eq_with(&self, other: &source::Line, arenas: &Arenas) -> bool {
        self.header
            .lookup_ref(&arenas.line_header)
            .eq_with(other, arenas)
            && self
                .impacted_objects
                .set_eq_by(&other.impacted_objects, |x, y| {
                    x.lookup_ref(&arenas.impacted_object).eq_with(y, arenas)
                })
    }
}

impl Line {
    pub fn from(arenas: &Arenas, source: source::Line) -> Self {
        Self {
            header: Interned::from(
                &arenas.line_header,
                LineHeader {
                    id: Interned::from(&arenas.string, source.id),
                    name: Interned::from(&arenas.string, source.name),
                    short_name: Interned::from(&arenas.string, source.short_name),
                    mode: Interned::from(&arenas.string, source.mode),
                    network_id: Interned::from(&arenas.string, source.network_id),
                },
            ),
            impacted_objects: InternedSet::new(source.impacted_objects.into_iter().map(|x| {
                let impacted_object = ImpactedObject::from(arenas, x);
                Interned::from(&arenas.impacted_object, impacted_object)
            })),
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Serialize_tuple, Deserialize_tuple, GetSize)]
pub struct LineHeader {
    pub id: IString,
    pub name: IString,
    pub short_name: IString,
    pub mode: IString,
    pub network_id: IString,
}

impl EqWith<source::Line, Arenas> for LineHeader {
    fn eq_with(&self, other: &source::Line, arenas: &Arenas) -> bool {
        self.id.eq_with(&other.id, &arenas.string)
            && self.name.eq_with(&other.name, &arenas.string)
            && self.short_name.eq_with(&other.short_name, &arenas.string)
            && self.mode.eq_with(&other.mode, &arenas.string)
            && self.network_id.eq_with(&other.network_id, &arenas.string)
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Serialize_tuple, Deserialize_tuple, GetSize)]
pub struct ImpactedObject {
    pub object: Interned<Object>,
    pub disruption_ids: Interned<InternedSet<Uuid>>,
}

impl EqWith<source::ImpactedObject, Arenas> for ImpactedObject {
    fn eq_with(&self, other: &source::ImpactedObject, arenas: &Arenas) -> bool {
        self.object
            .lookup_ref(&arenas.object)
            .eq_with(other, arenas)
            && self
                .disruption_ids
                .lookup_ref(&arenas.uuid_set)
                .set_eq_by(&other.disruption_ids, |x, y| x.eq_with(y, &arenas.uuid))
    }
}

impl ImpactedObject {
    pub fn from(arenas: &Arenas, source: source::ImpactedObject) -> Self {
        let disruption_ids = InternedSet::new(
            source
                .disruption_ids
                .into_iter()
                .map(|x| Interned::from(&arenas.uuid, x)),
        );
        Self {
            object: Interned::from(
                &arenas.object,
                Object {
                    typ: Interned::from(&arenas.string, source.typ),
                    id: Interned::from(&arenas.string, source.id),
                    name: Interned::from(&arenas.string, source.name),
                },
            ),
            disruption_ids: Interned::from(&arenas.uuid_set, disruption_ids),
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Serialize_tuple, Deserialize_tuple, GetSize)]
pub struct Object {
    pub typ: IString,
    pub id: IString,
    pub name: IString,
}

impl EqWith<source::ImpactedObject, Arenas> for Object {
    fn eq_with(&self, other: &source::ImpactedObject, arenas: &Arenas) -> bool {
        self.typ.eq_with(&other.typ, &arenas.string)
            && self.id.eq_with(&other.id, &arenas.string)
            && self.name.eq_with(&other.name, &arenas.string)
    }
}
