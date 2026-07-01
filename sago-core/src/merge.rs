//! Three-way schema merge.
//!
//! When two pipelines evolve the same dataset independently, their schemas
//! diverge from a common ancestor. A naive "last write wins" silently drops one
//! side's work; a plain two-way diff can't tell an *addition* from the *other
//! side's deletion*. A three-way merge compares both edits against the shared
//! `base` and reconciles them field-by-field, flagging only the genuinely
//! conflicting changes.
//!
//! For each field name seen in any of `base` / `ours` / `theirs` we classify the
//! per-side change relative to `base` ([`FieldChange`]) and combine the two
//! sides:
//!   * neither side changed → keep base
//!   * exactly one side changed → take that side
//!   * both sides made the *same* change → take it (no conflict)
//!   * both sides changed *differently* → [`MergeConflict`]
//!
//! The result is a best-effort merged [`Schema`] (conflicting fields fall back
//! to the `ours` value so the schema stays usable) plus the conflict list, so
//! callers can choose to fail, prompt, or auto-resolve.

use std::collections::BTreeSet;

use arrow::datatypes::{Field, Schema};
use serde::{Deserialize, Serialize};

/// How a single field changed on one side relative to the common ancestor.
#[derive(Debug, Clone, PartialEq, Eq)]
enum FieldChange {
    /// Present and identical to base (or absent in both).
    Unchanged,
    /// Absent in base, present on this side.
    Added,
    /// Present in base, absent on this side.
    Removed,
    /// Present in both but the type and/or nullability differs.
    Modified,
}

/// The kind of irreconcilable disagreement between the two sides.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictKind {
    /// Both sides added the same field name with different definitions.
    AddAdd,
    /// Both sides modified the field, but to different definitions.
    ModifyModify,
    /// One side removed the field while the other modified it.
    RemoveModify,
}

/// A single field on which `ours` and `theirs` disagree and which could not be
/// auto-resolved. The string fields are the debug definition of the field on
/// each side (`None` = the field is absent on that side).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeConflict {
    pub field_name: String,
    pub kind: ConflictKind,
    pub base: Option<String>,
    pub ours: Option<String>,
    pub theirs: Option<String>,
}

/// Outcome of a three-way merge: the reconciled schema plus any conflicts.
///
/// `#[must_use]`: the `merged` schema is populated even when there are
/// conflicts (conflicting fields fall back to `ours`), so silently dropping this
/// value would discard the conflict list and treat a lossy merge as clean.
/// Check [`is_clean`](Self::is_clean) / [`conflicts`](Self::conflicts) first.
#[derive(Debug, Clone)]
#[must_use = "a MergeResult may carry conflicts; inspect is_clean()/conflicts before using `merged`"]
pub struct MergeResult {
    /// Best-effort merged schema. Auto-resolved changes are applied; conflicting
    /// fields take the `ours` definition so the schema remains constructible.
    pub merged: Schema,
    /// Fields that could not be auto-merged. Empty ⇒ a clean merge.
    pub conflicts: Vec<MergeConflict>,
}

impl MergeResult {
    /// `true` when the merge produced no conflicts.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.conflicts.is_empty()
    }
}

/// Render a field's definition (type + nullability) for conflict reporting.
fn describe(field: &Field) -> String {
    format!(
        "{:?}{}",
        field.data_type(),
        if field.is_nullable() {
            " (nullable)"
        } else {
            " (non-null)"
        }
    )
}

/// Two fields are "the same definition" when type *and* nullability agree.
/// (Name equality is implied — they are keyed by name.)
fn same_def(a: &Field, b: &Field) -> bool {
    a.data_type() == b.data_type() && a.is_nullable() == b.is_nullable()
}

fn classify(base: Option<&Field>, side: Option<&Field>) -> FieldChange {
    match (base, side) {
        (None, None) => FieldChange::Unchanged,
        (None, Some(_)) => FieldChange::Added,
        (Some(_), None) => FieldChange::Removed,
        (Some(b), Some(s)) => {
            if same_def(b, s) {
                FieldChange::Unchanged
            } else {
                FieldChange::Modified
            }
        }
    }
}

/// Merge `ours` and `theirs` against their common ancestor `base`.
///
/// Field order in the merged schema follows a stable sorted order by name so
/// the output is deterministic regardless of input ordering.
pub fn three_way_merge(base: &Schema, ours: &Schema, theirs: &Schema) -> MergeResult {
    // Union of every field name across all three schemas, sorted for determinism.
    let mut names: BTreeSet<String> = BTreeSet::new();
    for schema in [base, ours, theirs] {
        for f in schema.fields() {
            names.insert(f.name().clone());
        }
    }

    let mut merged_fields: Vec<Field> = Vec::new();
    let mut conflicts: Vec<MergeConflict> = Vec::new();

    for name in &names {
        let b = base.field_with_name(name).ok();
        let o = ours.field_with_name(name).ok();
        let t = theirs.field_with_name(name).ok();

        let ours_change = classify(b, o);
        let theirs_change = classify(b, t);

        use FieldChange::*;
        match (&ours_change, &theirs_change) {
            // Nobody touched it (or both absent). Keep base if present.
            (Unchanged, Unchanged) => {
                if let Some(f) = b {
                    merged_fields.push(f.clone());
                }
            }

            // Exactly one side changed → take the changed side.
            (Unchanged, _) => push_side(&mut merged_fields, t),
            (_, Unchanged) => push_side(&mut merged_fields, o),

            // Both removed → stays removed, no conflict.
            (Removed, Removed) => {}

            // Both added or both modified: clean iff they agree.
            (Added, Added) | (Modified, Modified) => match (o, t) {
                (Some(of), Some(tf)) if same_def(of, tf) => merged_fields.push(of.clone()),
                _ => {
                    conflicts.push(MergeConflict {
                        field_name: name.clone(),
                        kind: if ours_change == Added {
                            ConflictKind::AddAdd
                        } else {
                            ConflictKind::ModifyModify
                        },
                        base: b.map(describe),
                        ours: o.map(describe),
                        theirs: t.map(describe),
                    });
                    // Keep `ours` so the merged schema stays usable.
                    push_side(&mut merged_fields, o);
                }
            },

            // One removed, the other modified → genuine conflict.
            (Removed, Modified) | (Modified, Removed) => {
                conflicts.push(MergeConflict {
                    field_name: name.clone(),
                    kind: ConflictKind::RemoveModify,
                    base: b.map(describe),
                    ours: o.map(describe),
                    theirs: t.map(describe),
                });
                // Prefer keeping the column (the modified side) over dropping it.
                push_side(&mut merged_fields, o.or(t));
            }

            // Remaining mixed combinations involving Added are impossible:
            // `Added` requires the field to be absent in base, but `Removed`/
            // `Modified` both require it to be present in base. Treat defensively
            // by taking whichever side has the field.
            _ => push_side(&mut merged_fields, o.or(t)),
        }
    }

    MergeResult {
        merged: Schema::new(merged_fields),
        conflicts,
    }
}

fn push_side(out: &mut Vec<Field>, field: Option<&Field>) {
    if let Some(f) = field {
        out.push(f.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    fn schema(fields: Vec<Field>) -> Schema {
        Schema::new(fields)
    }

    fn f(name: &str, dt: DataType, nullable: bool) -> Field {
        Field::new(name, dt, nullable)
    }

    fn names(schema: &Schema) -> Vec<String> {
        schema.fields().iter().map(|f| f.name().clone()).collect()
    }

    // ── clean merges ───────────────────────────────────────────────────────────

    #[test]
    fn test_no_changes_keeps_base() {
        let base = schema(vec![f("id", DataType::Int64, false)]);
        let result = three_way_merge(&base, &base, &base);
        assert!(result.is_clean());
        assert_eq!(names(&result.merged), vec!["id"]);
    }

    #[test]
    fn test_non_overlapping_additions_merge_cleanly() {
        let base = schema(vec![f("id", DataType::Int64, false)]);
        let ours = schema(vec![
            f("id", DataType::Int64, false),
            f("email", DataType::Utf8, true),
        ]);
        let theirs = schema(vec![
            f("id", DataType::Int64, false),
            f("age", DataType::Int32, true),
        ]);
        let result = three_way_merge(&base, &ours, &theirs);
        assert!(result.is_clean(), "conflicts: {:?}", result.conflicts);
        assert_eq!(names(&result.merged), vec!["age", "email", "id"]); // sorted
    }

    #[test]
    fn test_one_side_modifies_other_unchanged_takes_change() {
        let base = schema(vec![f("amount", DataType::Int32, false)]);
        let ours = schema(vec![f("amount", DataType::Int64, false)]); // widened
        let theirs = base.clone();
        let result = three_way_merge(&base, &ours, &theirs);
        assert!(result.is_clean());
        assert_eq!(
            result.merged.field_with_name("amount").unwrap().data_type(),
            &DataType::Int64
        );
    }

    #[test]
    fn test_identical_modification_on_both_sides_is_clean() {
        let base = schema(vec![f("amount", DataType::Int32, false)]);
        let changed = schema(vec![f("amount", DataType::Int64, false)]);
        let result = three_way_merge(&base, &changed, &changed);
        assert!(result.is_clean());
        assert_eq!(
            result.merged.field_with_name("amount").unwrap().data_type(),
            &DataType::Int64
        );
    }

    #[test]
    fn test_both_remove_same_field_is_clean() {
        let base = schema(vec![
            f("id", DataType::Int64, false),
            f("legacy", DataType::Utf8, true),
        ]);
        let trimmed = schema(vec![f("id", DataType::Int64, false)]);
        let result = three_way_merge(&base, &trimmed, &trimmed);
        assert!(result.is_clean());
        assert_eq!(names(&result.merged), vec!["id"]);
    }

    #[test]
    fn test_one_side_removes_other_unchanged_removes() {
        let base = schema(vec![
            f("id", DataType::Int64, false),
            f("legacy", DataType::Utf8, true),
        ]);
        let ours = schema(vec![f("id", DataType::Int64, false)]); // dropped legacy
        let theirs = base.clone();
        let result = three_way_merge(&base, &ours, &theirs);
        assert!(result.is_clean());
        assert_eq!(names(&result.merged), vec!["id"]);
    }

    #[test]
    fn test_identical_addition_on_both_sides_is_clean() {
        let base = schema(vec![f("id", DataType::Int64, false)]);
        let added = schema(vec![
            f("id", DataType::Int64, false),
            f("email", DataType::Utf8, true),
        ]);
        let result = three_way_merge(&base, &added, &added);
        assert!(result.is_clean());
        assert_eq!(names(&result.merged), vec!["email", "id"]);
    }

    // ── conflicts ────────────────────────────────────────────────────────────────

    #[test]
    fn test_add_add_conflict() {
        let base = schema(vec![f("id", DataType::Int64, false)]);
        let ours = schema(vec![
            f("id", DataType::Int64, false),
            f("tag", DataType::Utf8, true),
        ]);
        let theirs = schema(vec![
            f("id", DataType::Int64, false),
            f("tag", DataType::Int32, true), // same name, different type
        ]);
        let result = three_way_merge(&base, &ours, &theirs);
        assert_eq!(result.conflicts.len(), 1);
        let c = &result.conflicts[0];
        assert_eq!(c.field_name, "tag");
        assert_eq!(c.kind, ConflictKind::AddAdd);
        assert!(c.base.is_none());
        // Merged schema keeps `ours` so it stays usable.
        assert_eq!(
            result.merged.field_with_name("tag").unwrap().data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn test_modify_modify_conflict() {
        let base = schema(vec![f("amount", DataType::Int32, false)]);
        let ours = schema(vec![f("amount", DataType::Int64, false)]);
        let theirs = schema(vec![f("amount", DataType::Float64, false)]);
        let result = three_way_merge(&base, &ours, &theirs);
        assert_eq!(result.conflicts.len(), 1);
        let c = &result.conflicts[0];
        assert_eq!(c.kind, ConflictKind::ModifyModify);
        assert_eq!(c.base, Some(describe(&f("amount", DataType::Int32, false))));
        assert!(c.ours.as_ref().unwrap().contains("Int64"));
        assert!(c.theirs.as_ref().unwrap().contains("Float64"));
    }

    #[test]
    fn test_modify_modify_nullability_only_conflict() {
        let base = schema(vec![f("name", DataType::Utf8, false)]);
        let ours = schema(vec![f("name", DataType::Utf8, true)]); // made nullable
        let theirs = schema(vec![f("name", DataType::Utf8, false)]); // unchanged
        // Only one side changed → clean, takes the nullable version.
        let result = three_way_merge(&base, &ours, &theirs);
        assert!(result.is_clean());
        assert!(result.merged.field_with_name("name").unwrap().is_nullable());
    }

    #[test]
    fn test_remove_modify_conflict_keeps_column() {
        let base = schema(vec![f("status", DataType::Int32, false)]);
        let ours = schema(vec![f("status", DataType::Int64, false)]); // modified
        let theirs = schema(vec![] as Vec<Field>); // removed
        let result = three_way_merge(&base, &ours, &theirs);
        assert_eq!(result.conflicts.len(), 1);
        assert_eq!(result.conflicts[0].kind, ConflictKind::RemoveModify);
        // Conflict resolution keeps the column (the modified side).
        assert_eq!(names(&result.merged), vec!["status"]);
        assert_eq!(
            result.merged.field_with_name("status").unwrap().data_type(),
            &DataType::Int64
        );
    }

    #[test]
    fn test_multiple_mixed_changes() {
        // id: untouched. email: we add. age: they modify. legacy: both remove.
        // amount: conflicting modify/modify.
        let base = schema(vec![
            f("id", DataType::Int64, false),
            f("age", DataType::Int32, true),
            f("legacy", DataType::Utf8, true),
            f("amount", DataType::Int32, false),
        ]);
        let ours = schema(vec![
            f("id", DataType::Int64, false),
            f("age", DataType::Int32, true),
            f("amount", DataType::Int64, false), // modify
            f("email", DataType::Utf8, true),    // add
        ]);
        let theirs = schema(vec![
            f("id", DataType::Int64, false),
            f("age", DataType::Int64, true), // modify (Int32→Int64)
            f("amount", DataType::Float64, false), // modify (conflicts with ours)
        ]);
        let result = three_way_merge(&base, &ours, &theirs);

        // Only `amount` conflicts.
        assert_eq!(result.conflicts.len(), 1);
        assert_eq!(result.conflicts[0].field_name, "amount");

        let merged_names = names(&result.merged);
        assert!(merged_names.contains(&"email".to_string())); // our add kept
        assert!(!merged_names.contains(&"legacy".to_string())); // both removed
        // their modification of `age` applied
        assert_eq!(
            result.merged.field_with_name("age").unwrap().data_type(),
            &DataType::Int64
        );
    }

    #[test]
    fn test_merge_conflict_json_round_trip() {
        let c = MergeConflict {
            field_name: "amount".into(),
            kind: ConflictKind::ModifyModify,
            base: Some("Int32 (non-null)".into()),
            ours: Some("Int64 (non-null)".into()),
            theirs: Some("Float64 (non-null)".into()),
        };
        let json = serde_json::to_string(&c).unwrap();
        let back: MergeConflict = serde_json::from_str(&json).unwrap();
        assert_eq!(c, back);
    }
}
