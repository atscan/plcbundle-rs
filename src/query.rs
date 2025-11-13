use crate::options::QueryMode;
use anyhow::Result;
use sonic_rs::JsonValueTrait;

pub enum QueryEngine {
    JmesPath(jmespath::Expression<'static>),
    Simple(Vec<String>),
}

impl QueryEngine {
    pub fn new(query: &str, mode: QueryMode) -> Result<Self> {
        match mode {
            QueryMode::Simple => {
                let path: Vec<String> = query.split('.').map(|s| s.to_string()).collect();
                Ok(QueryEngine::Simple(path))
            }
            QueryMode::JmesPath => {
                let expr = jmespath::compile(query)
                    .map_err(|e| anyhow::anyhow!("Failed to compile JMESPath: {}", e))?;
                let expr = Box::leak(Box::new(expr));
                Ok(QueryEngine::JmesPath(expr.clone()))
            }
        }
    }

    pub fn query(&self, json_line: &str) -> Result<Option<String>> {
        match self {
            QueryEngine::Simple(path) => self.query_simple(json_line, path),
            QueryEngine::JmesPath(expr) => self.query_jmespath(json_line, expr),
        }
    }

    fn query_simple(&self, json_line: &str, path: &[String]) -> Result<Option<String>> {
        let data: sonic_rs::Value = sonic_rs::from_str(json_line)?;
        
        let mut current = &data;
        for key in path {
            match current.get(key) {
                Some(v) => current = v,
                None => return Ok(None),
            }
        }

        if current.is_null() {
            return Ok(None);
        }

        let output = if current.is_str() {
            current.as_str().unwrap().to_string()
        } else {
            sonic_rs::to_string(current)?
        };

        Ok(Some(output))
    }

    fn query_jmespath(&self, json_line: &str, expr: &jmespath::Expression<'_>) -> Result<Option<String>> {
        let data = jmespath::Variable::from_json(json_line)
            .map_err(|e| anyhow::anyhow!("Failed to parse JSON: {}", e))?;
        
        let result = expr.search(&data)
            .map_err(|e| anyhow::anyhow!("JMESPath search failed: {}", e))?;
        
        if result.is_null() {
            return Ok(None);
        }

        let output = if result.is_string() {
            result.as_string().unwrap().to_string()
        } else {
            // Convert jmespath result to JSON string via serde_json
            serde_json::to_string(&result)
                .map_err(|e| anyhow::anyhow!("Failed to serialize result: {}", e))?
        };

        Ok(Some(output))
    }
}
