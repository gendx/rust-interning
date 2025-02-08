use super::source;
use crate::intern::{EqWith, IString, Interned, Interner, StringInterner};
use crate::size::EstimateSize;
use std::hash::Hash;

#[derive(Default)]
pub struct Interners {
    string: StringInterner,
    disruption: Interner<Disruption>,
    line: Interner<Line>,
    application_period: Interner<ApplicationPeriod>,
    impacted_object: Interner<ImpactedObject>,
}

impl EstimateSize for Interners {
    fn allocated_bytes(&self) -> usize {
        self.string.allocated_bytes()
            + self.disruption.allocated_bytes()
            + self.line.allocated_bytes()
            + self.application_period.allocated_bytes()
            + self.impacted_object.allocated_bytes()
    }
}

impl Interners {
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
pub struct Data {
    pub status_code: Option<i32>,
    pub error: Option<IString>,
    pub message: Option<IString>,
    pub disruptions: Option<InternedSet<Disruption>>,
    pub lines: Option<InternedSet<Line>>,
    pub last_updated_date: Option<IString>,
}

impl EstimateSize for Data {
    fn allocated_bytes(&self) -> usize {
        self.status_code.allocated_bytes()
            + self.error.allocated_bytes()
            + self.message.allocated_bytes()
            + self.disruptions.allocated_bytes()
            + self.lines.allocated_bytes()
            + self.last_updated_date.allocated_bytes()
    }
}

impl EqWith<source::Data, Interners> for Data {
    fn eq_with(&self, other: &source::Data, interners: &Interners) -> bool {
        self.status_code == other.status_code
            && option_eq_by(&self.error, &other.error, |x, y| {
                x.eq_with(y, &interners.string)
            })
            && option_eq_by(&self.message, &other.message, |x, y| {
                x.eq_with(y, &interners.string)
            })
            && option_eq_by(&self.disruptions, &other.disruptions, |x, y| {
                x.set_eq_by(y, |x, y| {
                    x.eq_with_more(y, &interners.disruption, interners)
                })
            })
            && option_eq_by(&self.lines, &other.lines, |x, y| {
                x.set_eq_by(y, |x, y| x.eq_with_more(y, &interners.line, interners))
            })
            && option_eq_by(&self.last_updated_date, &other.last_updated_date, |x, y| {
                x.eq_with(y, &interners.string)
            })
    }
}

impl Data {
    pub fn from(interners: &mut Interners, source: source::Data) -> Self {
        Self {
            status_code: source.status_code,
            error: source
                .error
                .map(|x| Interned::from(&mut interners.string, x)),
            message: source
                .message
                .map(|x| Interned::from(&mut interners.string, x)),
            disruptions: source.disruptions.map(|x| {
                InternedSet::new(x.into_iter().map(|x| {
                    let disruption = Disruption::from(interners, x);
                    Interned::from(&mut interners.disruption, disruption)
                }))
            }),
            lines: source.lines.map(|x| {
                InternedSet::new(x.into_iter().map(|x| {
                    let line = Line::from(interners, x);
                    Interned::from(&mut interners.line, line)
                }))
            }),
            last_updated_date: source
                .last_updated_date
                .map(|x| Interned::from(&mut interners.string, x)),
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
    pub id: IString,
    pub name: IString,
    pub short_name: IString,
    pub mode: IString,
    pub network_id: IString,
    pub impacted_objects: InternedSet<ImpactedObject>,
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

impl EqWith<source::Line, Interners> for Line {
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
            id: Interned::from(&mut interners.string, source.id),
            name: Interned::from(&mut interners.string, source.name),
            short_name: Interned::from(&mut interners.string, source.short_name),
            mode: Interned::from(&mut interners.string, source.mode),
            network_id: Interned::from(&mut interners.string, source.network_id),
            impacted_objects: InternedSet::new(source.impacted_objects.into_iter().map(|x| {
                let impacted_object = ImpactedObject::from(interners, x);
                Interned::from(&mut interners.impacted_object, impacted_object)
            })),
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct ImpactedObject {
    pub typ: IString,
    pub id: IString,
    pub name: IString,
    pub disruption_ids: InternedSet<String>,
}

impl EstimateSize for ImpactedObject {
    fn allocated_bytes(&self) -> usize {
        self.typ.allocated_bytes()
            + self.id.allocated_bytes()
            + self.name.allocated_bytes()
            + self.disruption_ids.allocated_bytes()
    }
}

impl EqWith<source::ImpactedObject, Interners> for ImpactedObject {
    fn eq_with(&self, other: &source::ImpactedObject, interners: &Interners) -> bool {
        self.typ.eq_with(&other.typ, &interners.string)
            && self.id.eq_with(&other.id, &interners.string)
            && self.name.eq_with(&other.name, &interners.string)
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
            typ: Interned::from(&mut interners.string, source.typ),
            id: Interned::from(&mut interners.string, source.id),
            name: Interned::from(&mut interners.string, source.name),
            disruption_ids: InternedSet::new(
                source
                    .disruption_ids
                    .into_iter()
                    .map(|x| Interned::from(&mut interners.string, x)),
            ),
        }
    }
}
