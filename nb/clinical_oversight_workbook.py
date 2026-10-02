# Databricks notebook source
# MAGIC %md
# MAGIC # REACH Clinical Oversight Workbook
# MAGIC
# MAGIC Clinical oversight review queue for Jenn Datzko, Lashiniqua Howard, and Laura Brantman Johnson. The workbook uses the three clinician NPIs and limits the review queue to `lc_status_current = active - engaged`.

# COMMAND ----------

# MAGIC %pip install --force-reinstall --no-deps /Workspace/Users/kcorwin@harmonycares.com/reach/lib/reach-0.1.0-py3-none-any.whl

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from pathlib import Path

from reach._notes._clinical_oversight import (
    ACTIVE_ENGAGED_LIFECYCLE_STATUS,
    DEFAULT_NOTES_TABLE,
    DISCUSSION_GROUP_CLINICIAN_NPIS,
    DISCUSSION_GROUP_CLINICIANS,
    parse_discussed_identifiers,
    patient_review_html,
    patient_review_views,
    save_patient_review_note_by_hcmpi,
    seed_discussed_patient_notes,
)
from reach._pipes._oversight_runner import run_reach_oversight_pipe

NOTES_TABLE = DEFAULT_NOTES_TABLE
CLINICIAN_DISCUSSION_GROUP_NAMES = DISCUSSION_GROUP_CLINICIANS
CLINICIAN_DISCUSSION_GROUP_NPIS = DISCUSSION_GROUP_CLINICIAN_NPIS
pipe_run = run_reach_oversight_pipe(spark, notes_table=NOTES_TABLE, active_only=True, clinician_npis=CLINICIAN_DISCUSSION_GROUP_NPIS, require_non_empty=False)
outputs = pipe_run["outputs"]


# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipe Output Tables
# MAGIC
# MAGIC Materialized clinical oversight tables and row counts from this run.

# COMMAND ----------

display(pipe_run["summary"].orderBy("output_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seed Already Discussed Patients
# MAGIC
# MAGIC Reads the ignored `discussed.txt` file when present and inserts only missing reviewed-note rows.

# COMMAND ----------

DISCUSSION_SEED_PATHS = (
    Path("discussed.txt"),
    Path("/Workspace/Users/kcorwin@harmonycares.com/reach/discussed.txt"),
    Path("/home/care/reach/discussed.txt"),
)
discussion_seed_path = next((path for path in DISCUSSION_SEED_PATHS if path.exists()), None)

if discussion_seed_path is not None:
    discussed_identifiers = parse_discussed_identifiers(discussion_seed_path.read_text())
    seeded_discussions = seed_discussed_patient_notes(
        spark,
        discussed_identifiers,
        worklist=outputs["worklist"],
        notes_table=NOTES_TABLE,
        reviewed_by="clinical oversight workgroup",
    )
    display(seeded_discussions)
    pipe_run = run_reach_oversight_pipe(spark, notes_table=NOTES_TABLE, active_only=True, clinician_npis=CLINICIAN_DISCUSSION_GROUP_NPIS, require_non_empty=False)
    outputs = pipe_run["outputs"]
else:
    print("No discussed.txt seed file found; skipping already-discussed seed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinician Queue Summary
# MAGIC
# MAGIC Active-engaged patients available for review by clinician, with reviewed and remaining counts from the notes table.

# COMMAND ----------

displayHTML(f"<div style='font-size:14px;color:#374151;margin:4px 0 10px;'>Lifecycle filter: <strong>lc_status_current = {ACTIVE_ENGAGED_LIFECYCLE_STATUS}</strong></div>")
display(outputs["clinician_summary"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Active-Engaged Patients By Clinician
# MAGIC
# MAGIC Each clinician panel is shown separately for live workgroup review.

# COMMAND ----------

for clinician_name, clinician_panel in outputs["clinician_open_panels"].items():
    displayHTML(f"<h3 style='margin-top:18px;margin-bottom:6px;'>{clinician_name}</h3>")
    display(clinician_panel)

# COMMAND ----------

# MAGIC %md
# MAGIC ## All Active-Engaged Patients Needing Review
# MAGIC
# MAGIC Combined queue excluding patients with a saved review note.

# COMMAND ----------

display(outputs["open_worklist"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Patient Review
# MAGIC
# MAGIC Set one HCMPI to review the patient in a compact summary plus supporting tables.

# COMMAND ----------

PATIENT_HCMPI = ""

if PATIENT_HCMPI.strip():
    displayHTML(patient_review_html(outputs["worklist"], PATIENT_HCMPI))
    patient_views = patient_review_views(outputs["worklist"], PATIENT_HCMPI)
    displayHTML("<h3 style='margin-top:18px;margin-bottom:6px;'>Overview</h3>")
    display(patient_views["overview"])
    displayHTML("<h3 style='margin-top:18px;margin-bottom:6px;'>Utilization And Risk</h3>")
    display(patient_views["utilization"])
    displayHTML("<h3 style='margin-top:18px;margin-bottom:6px;'>Alignment</h3>")
    display(patient_views["alignment"])
else:
    print("Set PATIENT_HCMPI, then run this cell.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Patient Review Note
# MAGIC
# MAGIC Set the HCMPI and note text. The patient, clinician, ranking, and alignment context come from the active-engaged worklist.

# COMMAND ----------

NOTE_HCMPI = ""
NOTE_TEXT = ""

if NOTE_HCMPI.strip() and NOTE_TEXT.strip():
    saved_note = save_patient_review_note_by_hcmpi(
        spark,
        outputs["worklist"],
        hcmpi=NOTE_HCMPI,
        note_text=NOTE_TEXT,
        notes_table=NOTES_TABLE,
    )
    display(saved_note)
    pipe_run = run_reach_oversight_pipe(spark, notes_table=NOTES_TABLE, active_only=True, clinician_npis=CLINICIAN_DISCUSSION_GROUP_NPIS, require_non_empty=False)
    outputs = pipe_run["outputs"]
    displayHTML(patient_review_html(outputs["worklist"], NOTE_HCMPI))
elif NOTE_HCMPI.strip():
    print("Add NOTE_TEXT before saving the review note.")
else:
    print("Set NOTE_HCMPI and NOTE_TEXT, then run this cell to save a review note.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reviewed Patients By Clinician
# MAGIC
# MAGIC Saved and seeded review notes, split by clinician for follow-up.

# COMMAND ----------

for clinician_name, clinician_panel in outputs["clinician_reviewed_panels"].items():
    displayHTML(f"<h3 style='margin-top:18px;margin-bottom:6px;'>{clinician_name}</h3>")
    display(clinician_panel)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reviewed Patients And Notes
# MAGIC
# MAGIC Combined reviewed list, including saved notes.

# COMMAND ----------

display(outputs["reviewed_patients"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Refresh Active-Engaged Queue

# COMMAND ----------

pipe_run = run_reach_oversight_pipe(spark, notes_table=NOTES_TABLE, active_only=True, clinician_npis=CLINICIAN_DISCUSSION_GROUP_NPIS, require_non_empty=False)
outputs = pipe_run["outputs"]
for clinician_name, clinician_panel in outputs["clinician_open_panels"].items():
    displayHTML(f"<h3 style='margin-top:18px;margin-bottom:6px;'>{clinician_name}</h3>")
    display(clinician_panel)
