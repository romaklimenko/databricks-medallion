# Databricks notebook source
# Setup: Upload CSV files from the bundle workspace to the landing volume.
# Copies data/batch_1/ and data/batch_2/ into the managed volume so all
# approaches can read from the same location.
#
# Schemas and volume are created by the bundle (resources.schemas / resources.volumes
# in databricks.yml), so this notebook only handles the file copy.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "medallion")
catalog = dbutils.widgets.get("catalog_name")

# COMMAND ----------

import os
import shutil

volume_path = f"/Volumes/{catalog}/landing/raw_files"

# Bundle syncs data/ into the workspace. Resolve path relative to this notebook.
notebook_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)
workspace_data_dir = os.path.normpath(
    os.path.join("/Workspace", os.path.dirname(notebook_path.lstrip("/")), "..", "data")
)

print(f"Source: {workspace_data_dir}")
print(f"Destination: {volume_path}")

# COMMAND ----------

for batch in ["batch_1", "batch_2"]:
    src_dir = os.path.join(workspace_data_dir, batch)
    dst_dir = os.path.join(volume_path, batch)
    os.makedirs(dst_dir, exist_ok=True)

    for fname in os.listdir(src_dir):
        if fname.endswith(".csv"):
            shutil.copy2(os.path.join(src_dir, fname), os.path.join(dst_dir, fname))
            print(f"  Copied {fname} -> {dst_dir}/{fname}")

# COMMAND ----------

# Verify
for batch in ["batch_1", "batch_2"]:
    print(f"\n{batch}:")
    for fname in os.listdir(os.path.join(volume_path, batch)):
        size = os.path.getsize(os.path.join(volume_path, batch, fname))
        print(f"  {fname} ({size} bytes)")
