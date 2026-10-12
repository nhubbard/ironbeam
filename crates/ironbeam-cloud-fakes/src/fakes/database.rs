use super::*;

// ============================================================================
// FakeDatabaseIO
// ============================================================================

#[derive(Clone)]
pub struct FakeDatabaseIO {
    tables: Arc<Mutex<HashMap<String, Vec<Row>>>>,
    schemas: SchemaMap,
}

impl FakeDatabaseIO {
    #[must_use]
    pub fn new() -> Self {
        Self {
            tables: Arc::new(Mutex::new(HashMap::new())),
            schemas: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Adds a table with schema to the fake database.
    ///
    /// # Panics
    ///
    /// Panics if the mutex protecting the tables or schemas is poisoned.
    pub fn add_table(&self, name: &str, schema: Vec<(String, String)>) {
        self.tables
            .lock()
            .expect("tables mutex poisoned")
            .insert(name.to_string(), Vec::new());
        self.schemas
            .lock()
            .expect("schemas mutex poisoned")
            .insert(name.to_string(), schema);
    }
}

impl Default for FakeDatabaseIO {
    fn default() -> Self {
        Self::new()
    }
}

impl DatabaseIO for FakeDatabaseIO {
    fn query(&self, sql: &str, _params: Vec<String>) -> CloudResult<Vec<Row>> {
        // Simple fake: assume the query is "SELECT * FROM table_name"
        let parts: Vec<&str> = sql.split_whitespace().collect();
        if parts.len() >= 4 && parts[0].eq_ignore_ascii_case("SELECT") {
            let table_name = parts[3];
            let tables = self.tables.lock().expect("tables mutex poisoned");

            tables.get(table_name).cloned().ok_or_else(|| {
                CloudIOError::new(ErrorKind::NotFound, format!("Table {table_name} not found"))
            })
        } else {
            Ok(Vec::new())
        }
    }

    fn execute(&self, sql: &str, _params: Vec<String>) -> CloudResult<u64> {
        // Simple fake: count INSERT/UPDATE/DELETE
        if sql.to_uppercase().contains("INSERT") {
            Ok(1)
        } else {
            Ok(0)
        }
    }

    fn begin_transaction(&self) -> CloudResult<Box<dyn Transaction>> {
        Ok(Box::new(FakeTransaction {
            db: self.clone(),
            committed: false,
        }))
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

struct FakeTransaction {
    db: FakeDatabaseIO,
    committed: bool,
}

impl Transaction for FakeTransaction {
    fn query(&mut self, sql: &str, params: Vec<String>) -> CloudResult<Vec<Row>> {
        self.db.query(sql, params)
    }

    fn execute(&mut self, sql: &str, params: Vec<String>) -> CloudResult<u64> {
        self.db.execute(sql, params)
    }

    fn commit(mut self: Box<Self>) -> CloudResult<()> {
        self.committed = true;
        Ok(())
    }

    fn rollback(self: Box<Self>) -> CloudResult<()> {
        Ok(())
    }
}
