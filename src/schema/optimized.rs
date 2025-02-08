use super::source;
use crate::intern::{EqWith, IString, Interned, Interner, StringInterner};
use crate::size::EstimateSize;
use std::hash::Hash;

#[derive(Default)]
pub struct Interners {
    string: StringInterner,
    disruption: Interner<Disruption>,
    line: Interner<Line>,
    line_header: Interner<LineHeader>,
    application_period: Interner<ApplicationPeriod>,
    impacted_object: Interner<ImpactedObject>,
    object: Interner<Object>,
}

impl EstimateSize for Interners {
    fn allocated_bytes(&self) -> usize {
        self.string.allocated_bytes()
            + self.disruption.allocated_bytes()
            + self.line.allocated_bytes()
            + self.line_header.allocated_bytes()
            + self.application_period.allocated_bytes()
            + self.impacted_object.allocated_bytes()
            + self.object.allocated_bytes()
    }
}

impl Interners {
    pub fn print_summary(&self, total_bytes: usize) {
        self.string.print_summary("", "String", total_bytes);
        self.disruption.print_summary("", "Disruption", total_bytes);
        self.application_period
            .print_summary("  ", "ApplicationPeriod", total_bytes);
        self.line.print_summary("", "Line", total_bytes);
        self.line_header
            .print_summary("  ", "LineHeader", total_bytes);
        self.impacted_object
            .print_summary("  ", "ImpactedObject", total_bytes);
        self.object.print_summary("    ", "Object", total_bytes);
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

#[derive(Debug, Hash, PartialEq, Eq)]
pub enum Data {
    Success {
        disruptions: InternedSet<Disruption>,
        lines: InternedSet<Line>,
        last_updated_date: IString,
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
                    disruptions.set_eq_by(other, |x, y| {
                        x.eq_with_more(y, &interners.disruption, interners)
                    })
                }) && other.lines.as_ref().is_some_and(|other| {
                    lines.set_eq_by(other, |x, y| x.eq_with_more(y, &interners.line, interners))
                }) && other
                    .last_updated_date
                    .as_ref()
                    .is_some_and(|other| last_updated_date.eq_with(other, &interners.string))
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
            } => Data::Success {
                disruptions: InternedSet::new(disruptions.into_iter().map(|x| {
                    let disruption = Disruption::from(interners, x);
                    Interned::from(&mut interners.disruption, disruption)
                })),
                lines: InternedSet::new(lines.into_iter().map(|x| {
                    let line = Line::from(interners, x);
                    Interned::from(&mut interners.line, line)
                })),
                last_updated_date: Interned::from(&mut interners.string, last_updated_date),
            },
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

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct Disruption {
    pub id: IString,
    pub application_periods: InternedSet<ApplicationPeriod>,
    pub last_update: IString,
    pub cause: IString,
    pub severity: IString,
    pub tags: Option<InternedSet<String>>,
    pub title: IString,
    pub message: IString,
    pub disruption_id: Option<IString>,
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
        self.id.eq_with(&other.id, &interners.string)
            && self
                .application_periods
                .set_eq_by(&other.application_periods, |x, y| {
                    x.eq_with_more(y, &interners.application_period, interners)
                })
            && self
                .last_update
                .eq_with(&other.last_update, &interners.string)
            && self.cause.eq_with(&other.cause, &interners.string)
            && self.severity.eq_with(&other.severity, &interners.string)
            && option_eq_by(&self.tags, &other.tags, |x, y| {
                x.set_eq_by(y, |x, y| x.eq_with(y, &interners.string))
            })
            && self.title.eq_with(&other.title, &interners.string)
            && self.message.eq_with(&other.message, &interners.string)
            && option_eq_by(&self.disruption_id, &other.disruption_id, |x, y| {
                x.eq_with(y, &interners.string)
            })
    }
}

impl Disruption {
    pub fn from(interners: &mut Interners, source: source::Disruption) -> Self {
        Self {
            id: Interned::from(&mut interners.string, source.id),
            application_periods: InternedSet::new(source.application_periods.into_iter().map(
                |x| {
                    let application_period = ApplicationPeriod::from(interners, x);
                    Interned::from(&mut interners.application_period, application_period)
                },
            )),
            last_update: Interned::from(&mut interners.string, source.last_update),
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
                .map(|x| Interned::from(&mut interners.string, x)),
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct ApplicationPeriod {
    pub begin: IString,
    pub end: IString,
}

impl EstimateSize for ApplicationPeriod {
    fn allocated_bytes(&self) -> usize {
        self.begin.allocated_bytes() + self.end.allocated_bytes()
    }
}

impl EqWith<source::ApplicationPeriod, Interners> for ApplicationPeriod {
    fn eq_with(&self, other: &source::ApplicationPeriod, interners: &Interners) -> bool {
        self.begin.eq_with(&other.begin, &interners.string)
            && self.end.eq_with(&other.end, &interners.string)
    }
}

impl ApplicationPeriod {
    pub fn from(interners: &mut Interners, source: source::ApplicationPeriod) -> Self {
        Self {
            begin: Interned::from(&mut interners.string, source.begin),
            end: Interned::from(&mut interners.string, source.end),
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
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

#[derive(Debug, Hash, PartialEq, Eq)]
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

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct ImpactedObject {
    pub object: Interned<Object>,
    pub disruption_ids: InternedSet<String>,
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
                .set_eq_by(&other.disruption_ids, |x, y| {
                    x.eq_with(y, &interners.string)
                })
    }
}

impl ImpactedObject {
    pub fn from(interners: &mut Interners, source: source::ImpactedObject) -> Self {
        Self {
            object: Interned::from(
                &mut interners.object,
                Object {
                    typ: Interned::from(&mut interners.string, source.typ),
                    id: Interned::from(&mut interners.string, source.id),
                    name: Interned::from(&mut interners.string, source.name),
                },
            ),
            disruption_ids: InternedSet::new(
                source
                    .disruption_ids
                    .into_iter()
                    .map(|x| Interned::from(&mut interners.string, x)),
            ),
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
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
