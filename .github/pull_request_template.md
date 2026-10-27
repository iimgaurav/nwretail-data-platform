## 📋 Pull Request Description
<!-- Provide a brief description of the pipeline, notebook, or schema changes introduced -->

### 🔄 Medallion Layer Impact
- [ ] **Raw -> Bronze**: Ingestion configurations, CSV source schema, or `PL_Ingest_Raw_To_Bronze`.
- [ ] **Bronze -> Silver**: Data cleansing, SCD Type-2 dimension logic, or `PL_BronzeToSilver`.
- [ ] **Silver -> Gold**: Dimensional star schema (`dim_*`, `fact_*`), analytics views, or `PL_SilverToGold`.
- [ ] **Master Orchestrator**: Scheduling, concurrency locks, or `PL_Master_Orchestrator`.
- [ ] **Infrastructure / ARM**: Linked services, datasets, or `arm-template/`.

---

## 🛡️ Production Quality Checklist
- [ ] **JSON Syntax**: All ADF pipelines, datasets, and linked services are valid JSON.
- [ ] **Idempotency**: Pipeline runs can be safely rerun or retried without duplicate data.
- [ ] **Locking & Audit**: `sp_LockPipeline` and `sp_UnlockPipeline` are invoked to avoid concurrency race conditions.
- [ ] **Zero Hardcoded Secrets**: Credentials use Azure Key Vault or Managed Identity (`MSI`).
- [ ] **Delta Table Integrity**: Schema enforcement and Z-ordering preserved on Delta targets.
