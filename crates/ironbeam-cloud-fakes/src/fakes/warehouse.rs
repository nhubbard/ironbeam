use super::*;

// ============================================================================
// FakeWarehouseIO
// ============================================================================

#[derive(Clone)]
pub struct FakeWarehouseIO {
    tables: Arc<Mutex<HashMap<String, Vec<Vec<String>>>>>,
    schemas: SchemaMap,
}

impl FakeWarehouseIO {
    #[must_use]
    pub fn new() -> Self {
        Self {
            tables: Arc::new(Mutex::new(HashMap::new())),
            schemas: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Adds a table with schema and data to the fake warehouse.
    ///
    /// # Panics
    ///
    /// Panics if the mutex protecting the tables or schemas is poisoned.
    pub fn add_table(&self, name: &str, schema: Vec<(String, String)>, data: Vec<Vec<String>>) {
        self.tables
            .lock()
            .expect("tables mutex poisoned")
            .insert(name.to_string(), data);
        self.schemas
            .lock()
            .expect("schemas mutex poisoned")
            .insert(name.to_string(), schema);
    }
}

impl Default for FakeWarehouseIO {
    fn default() -> Self {
        Self::new()
    }
}

impl WarehouseIO for FakeWarehouseIO {
    fn query(&self, sql: &str) -> CloudResult<QueryResult> {
        // Simple fake: assume the query is "SELECT * FROM table_name"
        let parts: Vec<&str> = sql.split_whitespace().collect();
        if parts.len() >= 4 && parts[0].eq_ignore_ascii_case("SELECT") {
            let table_name = parts[3];
            let tables = self.tables.lock().expect("tables mutex poisoned");
            let schemas = self.schemas.lock().expect("schemas mutex poisoned");

            if let (Some(data), Some(schema)) = (tables.get(table_name), schemas.get(table_name)) {
                let columns: Vec<String> = schema.iter().map(|(name, _)| name.clone()).collect();
                let row_count = data.len();
                let rows = data.clone();
                drop(tables);
                drop(schemas);
                Ok(QueryResult {
                    columns,
                    rows,
                    row_count,
                })
            } else {
                Err(CloudIOError::new(
                    ErrorKind::NotFound,
                    format!("Table {table_name} not found"),
                ))
            }
        } else {
            Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                row_count: 0,
            })
        }
    }

    fn execute(&self, _sql: &str) -> CloudResult<()> {
        Ok(())
    }

    fn load_data(
        &self,
        table: &str,
        _source_uri: &str,
        _options: HashMap<String, String>,
    ) -> CloudResult<()> {
        self.tables
            .lock()
            .expect("tables mutex poisoned")
            .entry(table.to_string())
            .or_default();
        Ok(())
    }

    fn export_data(
        &self,
        _sql: &str,
        _destination_uri: &str,
        _options: HashMap<String, String>,
    ) -> CloudResult<()> {
        Ok(())
    }

    fn table_exists(&self, table: &str) -> CloudResult<bool> {
        Ok(self
            .tables
            .lock()
            .expect("tables mutex poisoned")
            .contains_key(table))
    }

    fn get_schema(&self, table: &str) -> CloudResult<Vec<(String, String)>> {
        self.schemas
            .lock()
            .expect("schemas mutex poisoned")
            .get(table)
            .cloned()
            .ok_or_else(|| {
                CloudIOError::new(ErrorKind::NotFound, format!("Table {table} not found"))
            })
    }
}
