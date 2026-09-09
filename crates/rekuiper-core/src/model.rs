use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamRecord {
    pub timestamp: i64,
    pub data: HashMap<String, Value>,
}

impl StreamRecord {
    pub fn new(data: HashMap<String, Value>) -> Self {
        Self {
            timestamp: chrono::Utc::now().timestamp_millis(),
            data,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamDefinition {
    pub name: String,
    #[serde(default)]
    pub sql: String,
    #[serde(default)]
    pub options: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDefinition {
    pub name: String,
    #[serde(default)]
    pub sql: String,
    #[serde(default)]
    pub options: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleDefinition {
    pub id: String,
    #[serde(default)]
    pub sql: String,
    #[serde(default)]
    pub actions: Vec<HashMap<String, Value>>,
    #[serde(default)]
    pub options: Option<HashMap<String, Value>>,
    #[serde(default)]
    pub graph: Option<GraphDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct GraphDefinition {
    #[serde(default)]
    pub nodes: HashMap<String, GraphNode>,
    #[serde(default)]
    pub topo: GraphTopo,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct GraphNode {
    #[serde(rename = "type", default)]
    pub node_category: String, // "source", "operator", "sink"
    #[serde(rename = "nodeType", default)]
    pub node_type: String, // e.g. "mqtt", "filter", "pick", "window", "function", "aggfunc", "log"
    #[serde(default)]
    pub props: HashMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct GraphTopo {
    #[serde(default)]
    pub sources: Vec<String>,
    #[serde(default)]
    pub edges: HashMap<String, Vec<String>>,
}

/// Compile a visual rule DAG into an equivalent SQL string plus sink actions.
///
/// Resulting shape: `SELECT <projections or *> FROM <from> [WHERE
/// <combined_filters>] [GROUP BY <window>]`. Nodes are visited in sorted key
/// order so compilation is deterministic.
pub fn compile_graph_to_sql_and_actions(
    graph: &GraphDefinition,
) -> Result<(String, Vec<HashMap<String, Value>>), String> {
    fn prop_str(props: &HashMap<String, Value>, key: &str) -> Option<String> {
        props
            .get(key)
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    }

    fn prop_u64(props: &HashMap<String, Value>, key: &str, default: u64) -> u64 {
        if let Some(v) = props.get(key) {
            if let Some(n) = v.as_u64() {
                return n;
            }
            if let Some(n) = v.as_i64().and_then(|n| u64::try_from(n).ok()) {
                return n;
            }
            if let Some(s) = v.as_str() {
                if let Ok(n) = s.trim().parse::<u64>() {
                    return n;
                }
            }
        }
        default
    }

    // Source stream: prefer `sourceName`/`datasource` props of source nodes
    // (node key as fallback), else the topo entry points.
    let mut source_keys: Vec<&String> = graph
        .nodes
        .iter()
        .filter(|(_, n)| n.node_category == "source")
        .map(|(k, _)| k)
        .collect();
    source_keys.sort();
    let from = match source_keys.first() {
        Some(key) => {
            let node = &graph.nodes[*key];
            prop_str(&node.props, "sourceName")
                .or_else(|| prop_str(&node.props, "datasource"))
                .unwrap_or_else(|| (*key).clone())
        }
        None => graph
            .topo
            .sources
            .first()
            .cloned()
            .ok_or_else(|| "Graph rule has no source".to_string())?,
    };

    let mut filters: Vec<String> = Vec::new();
    let mut projections: Vec<String> = Vec::new();
    let mut window: Option<String> = None;
    let mut actions: Vec<HashMap<String, Value>> = Vec::new();

    let mut node_keys: Vec<&String> = graph.nodes.keys().collect();
    node_keys.sort();
    for key in node_keys {
        let node = &graph.nodes[key];
        match node.node_category.as_str() {
            "operator" => match node.node_type.as_str() {
                "filter" => {
                    if let Some(expr) = prop_str(&node.props, "expr") {
                        filters.push(format!("({})", expr));
                    }
                }
                "pick" => {
                    if let Some(fields) = node.props.get("fields").and_then(|v| v.as_array()) {
                        for field in fields {
                            if let Some(name) = field
                                .as_str()
                                .map(|s| s.trim().to_string())
                                .filter(|s| !s.is_empty())
                            {
                                projections.push(name);
                            }
                        }
                    }
                }
                "function" | "aggfunc" => {
                    if let Some(expr) = prop_str(&node.props, "expr") {
                        projections.push(expr);
                    }
                }
                "window" => {
                    let kind = node
                        .props
                        .get("type")
                        .and_then(|v| v.as_str())
                        .unwrap_or("tumblingwindow")
                        .to_ascii_lowercase();
                    let unit = node
                        .props
                        .get("unit")
                        .and_then(|v| v.as_str())
                        .unwrap_or("ss")
                        .to_ascii_lowercase();
                    let size = prop_u64(&node.props, "size", 10);
                    window = Some(match kind.as_str() {
                        "hoppingwindow" => {
                            let interval = if node.props.contains_key("interval") {
                                prop_u64(&node.props, "interval", size)
                            } else {
                                size
                            };
                            format!("HOPPINGWINDOW({}, {}, {})", unit, size, interval)
                        }
                        "slidingwindow" => format!("SLIDINGWINDOW({}, {})", unit, size),
                        "countwindow" => {
                            if node.props.contains_key("interval") {
                                format!(
                                    "COUNTWINDOW({}, {})",
                                    size,
                                    prop_u64(&node.props, "interval", size)
                                )
                            } else {
                                format!("COUNTWINDOW({})", size)
                            }
                        }
                        _ => format!("TUMBLINGWINDOW({}, {})", unit, size),
                    });
                }
                _ => {}
            },
            "sink" => {
                let action_kind = if node.node_type.is_empty() {
                    (*key).clone()
                } else {
                    node.node_type.clone()
                };
                let mut action = HashMap::new();
                action.insert(
                    action_kind,
                    Value::Object(node.props.clone().into_iter().collect()),
                );
                actions.push(action);
            }
            _ => {}
        }
    }

    let mut sql = format!(
        "SELECT {} FROM {}",
        if projections.is_empty() {
            "*".to_string()
        } else {
            projections.join(", ")
        },
        from
    );
    if !filters.is_empty() {
        sql.push_str(&format!(" WHERE {}", filters.join(" AND ")));
    }
    if let Some(window) = window {
        sql.push_str(&format!(" GROUP BY {}", window));
    }
    Ok((sql, actions))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_compile_graph_to_sql_and_actions() {
        let mut nodes = HashMap::new();
        nodes.insert(
            "src".to_string(),
            GraphNode {
                node_category: "source".to_string(),
                node_type: "stream".to_string(),
                props: [("sourceName".to_string(), json!("demo"))]
                    .into_iter()
                    .collect(),
            },
        );
        nodes.insert(
            "f".to_string(),
            GraphNode {
                node_category: "operator".to_string(),
                node_type: "filter".to_string(),
                props: [("expr".to_string(), json!("temp > 25"))]
                    .into_iter()
                    .collect(),
            },
        );
        nodes.insert(
            "p".to_string(),
            GraphNode {
                node_category: "operator".to_string(),
                node_type: "pick".to_string(),
                props: [("fields".to_string(), json!(["temp"]))].into_iter().collect(),
            },
        );
        nodes.insert(
            "out".to_string(),
            GraphNode {
                node_category: "sink".to_string(),
                node_type: "log".to_string(),
                props: HashMap::new(),
            },
        );
        let graph = GraphDefinition {
            nodes,
            topo: GraphTopo {
                sources: vec!["src".to_string()],
                edges: [
                    ("src".to_string(), vec!["f".to_string()]),
                    ("f".to_string(), vec!["p".to_string()]),
                    ("p".to_string(), vec!["out".to_string()]),
                ]
                .into_iter()
                .collect(),
            },
        };
        let (sql, actions) = compile_graph_to_sql_and_actions(&graph).unwrap();
        assert_eq!(sql, "SELECT temp FROM demo WHERE (temp > 25)");
        assert_eq!(actions.len(), 1);
        assert!(actions[0].contains_key("log"));
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuleStatus {
    pub status: String,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub source_records_in_total: u64,
    #[serde(default)]
    pub sink_records_out_total: u64,
    #[serde(default)]
    pub exceptions_total: u64,
}

impl Default for RuleStatus {
    fn default() -> Self {
        Self {
            status: "running".to_string(),
            message: "".to_string(),
            source_records_in_total: 0,
            sink_records_out_total: 0,
            exceptions_total: 0,
        }
    }
}
