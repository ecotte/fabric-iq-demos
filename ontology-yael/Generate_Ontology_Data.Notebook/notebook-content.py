# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c1ba4a21-99f0-4e81-a8fb-8a2a232fcd18",
# META       "default_lakehouse_name": "LHOntologyRetail",
# META       "default_lakehouse_workspace_id": "c6ad9930-ffad-4cfc-b8bd-33fef579b11a",
# META       "known_lakehouses": [
# META         {
# META           "id": "c1ba4a21-99f0-4e81-a8fb-8a2a232fcd18"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Fabric IQ Accelerator Sample
#         
# ### Generate Delta tables from Ontology Package

# CELL ********************

# Install Fabric IQ Ontology Accelerator Package
%pip install /lakehouse/default/Files/fabriciq_ontology_accelerator-0.1.0-py3-none-any.whl --q

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sempy.fabric as fabric
import json
from fabricontology.generate_data import generate_instance_data, generate_events_data
from notebookutils import mssparkutils

ontology_package_path = "/lakehouse/default/Files/retail_ontology_package.iq"

# Create delta tables in the default lakehouse
lakehouse_schema = "dbo"  # replace this if using lakehouse without schemas.
response = generate_instance_data(spark, ontology_package_path=ontology_package_path, database=lakehouse_schema, mode="overwrite")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create kusto tables 
eventhouse_cluster_uri = "https://trd-2cyqxwtqm58xnp1pej.z2.kusto.fabric.microsoft.com"
eventhouse_database = "EHOntologyRetail"
access_token=mssparkutils.credentials.getToken(eventhouse_cluster_uri)

response = generate_events_data(spark, 
        ontology_package_path=ontology_package_path,
        eventhouse_cluster_uri=eventhouse_cluster_uri,
        eventhouse_database=eventhouse_database,
        access_token=access_token )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
