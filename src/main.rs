
use std::env;
use anyhow::{anyhow, Result};
use reqwest::header::{AUTHORIZATION, USER_AGENT};
use serde::Deserialize;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time:: Duration;
use chrono::{DateTime, Utc};
use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Layout},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Tabs},
    text::{Line, Span},
    Terminal,
    style::{Color, Style},
};
use std::io;

#[derive(Debug, Deserialize)]
struct JobSettings {
    name: String,
}

#[derive(Debug, Deserialize)]
struct JobResponse {
    settings: JobSettings,
}

#[derive(Debug)]
struct Issue {
    run_id: i64,
    start_time_at_ms: Option<i64>,
    severity: Severity,
    issue_type: IssueType,
    message: String,
    workflow_name: Option<String>,
    run_page_url: Option<String>,
}

impl Issue {
    fn from_job_run(job_run: &JobRun) -> Self {
        let result_state = job_run
            .state
            .as_ref()
            .and_then(|s| s.result_state.as_deref());

        let state_message = job_run
            .state
            .as_ref()
            .and_then(|s| s.state_message.as_deref())
            .unwrap_or("No state message returned by Databricks");

        let issue_type = match result_state {
            Some("FAILED") => IssueType::RunFailed,
            Some("TIMEDOUT") => IssueType::RunTimedOut,
            Some("INTERNAL_ERROR") => IssueType::InternalError,
            _ => IssueType::UnknownFailure,
        };

        Issue {
            run_id: job_run.run_id,
            start_time_at_ms: job_run.start_time,
            severity: Severity::Error,
            issue_type,
            message: state_message.to_string(),
            workflow_name: job_run.workflow_name.clone(),
            run_page_url: job_run.run_page_url.clone(),
        }

    }
}

#[derive(Debug)]
enum Severity {
    Error,
    Warning,
}

#[derive(Debug)]
enum IssueType {
    RunFailed,
    RunTimedOut,
    InternalError,
    ClusterProblem,
    UnknownFailure,
}
#[derive(Debug)]
struct EnvVariables {
    workspace_url: String,
    pat: String,
    job_ids: Vec<String>,
    refresh_rate_minutes: i64,
    debug: bool,
}

#[derive(Debug, Deserialize)]
struct RunState {
    life_cycle_state: Option<String>,
    result_state: Option<String>,
    state_message: Option<String>,
}

#[derive(Debug, Deserialize)]
struct JobRunsResponse {
    runs: Vec<JobRun>,
}

#[derive(Debug, Clone, Copy)]
enum TimeWindow {
    Last24h,
    Last7d,
}

#[derive(Debug)]
struct WorkflowIssues {
    workflow_name: String,
    issues: Vec<Issue>,
}

impl WorkflowIssues {
    fn has_24h_issues(&self) -> bool {
        let now = now_milli_seconds();
        self.issues.iter().any(|issue| is_in_last_24h(issue, now))
    }
}

#[derive(Debug)]
struct AppState {
    workflows: Vec<WorkflowIssues>,
    selected_workflow_index: usize,
    selected_issue_index: usize,
    current_window: TimeWindow,
    last_refresh_ms: Option<i64>,
    should_quit: bool,
    needs_refresh: bool,
}

impl AppState {
    fn selected_workflow(&self) -> Option<&WorkflowIssues> {
        self.workflows.get(self.selected_workflow_index)
    }

    fn visible_issues(&self) -> Vec<&Issue> {
        let now = now_milli_seconds();

        self.selected_workflow()
            .map(|workflow| {
                workflow
                    .issues
                    .iter()
                    .filter(|issue| match self.current_window {
                        TimeWindow::Last24h => is_in_last_24h(issue, now),
                        TimeWindow::Last7d => is_in_last_7d(issue, now),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn selected_issue(&self) -> Option<&Issue> {
        let visible = self.visible_issues();
        visible.get(self.selected_issue_index).copied()
    }
}

#[derive(Debug, Deserialize)]
struct JobRun {
    run_id: i64,
    start_time: Option<i64>, // milliseconds since epoch
    end_time: Option<i64>, // milliseconds since epoch
    state: Option<RunState>,
    workflow_name: Option<String>,
    run_page_url: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let workflow_config = parse_env_variables()?;
    let mut app = build_app_state(&workflow_config).await?;
    let mut terminal = setup_terminal()?;

    let result = run_app(&mut terminal, &mut app, &workflow_config).await;

    restore_terminal(terminal)?;
    result
}

async fn run_app(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut AppState,
    workflow_config: &EnvVariables,
) -> Result<()> {
    let refresh_every = Duration::from_secs((workflow_config.refresh_rate_minutes * 60) as u64);
    let mut last_refresh = std::time::Instant::now();

    loop {
        terminal.draw(|frame| draw(frame, app))?;

        if event::poll(std::time::Duration::from_millis(200))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    handle_key_event(app, key.code);
                }
            }
        }
        if app.needs_refresh {
            refresh_app(app,workflow_config).await?;
            app.needs_refresh = false;
            last_refresh = std::time::Instant::now();
        }
        if last_refresh.elapsed() >= refresh_every {
            refresh_app(app, workflow_config).await?;
            last_refresh = std::time::Instant::now();
        }

        if app.should_quit {
            break;
        }
    }

    Ok(())
}

async fn refresh_app(app: &mut AppState, workflow_config: &EnvVariables) -> Result<()> {
    app.workflows = load_workflows(workflow_config).await?;
    app.last_refresh_ms = Some(now_milli_seconds());

    if app.selected_workflow_index >= app.workflows.len() {
        app.selected_workflow_index = app.workflows.len().saturating_sub(1);
    }

    let visible_len = app.visible_issues().len();
    if app.selected_issue_index >= visible_len {
        app.selected_issue_index = visible_len.saturating_sub(1);
    }

    Ok(())
}

fn draw_header(frame: &mut ratatui::Frame, area: ratatui::layout::Rect, app: &AppState) {
    let last_refresh = app
        .last_refresh_ms
        .map(format_timestamp)
        .unwrap_or_else(|| "never".to_string());

    let visible_count = app.visible_issues().len();

    let workflow_name = app
        .selected_workflow()
        .map(|w| w.workflow_name.as_str())
        .unwrap_or("none");

    let window = match app.current_window {
        TimeWindow::Last24h => "24h",
        TimeWindow::Last7d => "7d",
    };

    let text = format!(
        "Workflow Monitor | Workflow: {workflow_name} | Window: {window} | Visible issues: {visible_count} | Last refresh: {last_refresh}"
    );

    frame.render_widget(Paragraph::new(text), area);
}

fn draw_workflow_tabs(frame: &mut ratatui::Frame, area: ratatui::layout::Rect, app: &AppState) {
    let titles: Vec<Line> = app
        .workflows
        .iter()
        .map(|w| {
            if w.has_24h_issues() {
                Line::from(Span::styled(
                    format!("{} !", w.workflow_name),
                    Style::default().fg(Color::Red),
                ))
            } else {
                Line::from(Span::raw(w.workflow_name.clone()))
            }
        })
        .collect();

    let tabs = Tabs::new(titles)
        .select(app.selected_workflow_index)
        .block(Block::default().borders(Borders::ALL).title("Workflows"));

    frame.render_widget(tabs, area);
}

fn draw_time_window_tabs(frame: &mut ratatui::Frame, area: ratatui::layout::Rect, app: &AppState) {
    let titles = vec![Line::from("24h"), Line::from("7d")];

    let selected = match app.current_window {
        TimeWindow::Last24h => 0,
        TimeWindow::Last7d => 1,
    };

    let tabs = Tabs::new(titles)
        .select(selected)
        .block(Block::default().borders(Borders::ALL).title("Window"));

    frame.render_widget(tabs, area);
}



fn handle_key_event(app: &mut AppState, code: KeyCode) {
    match code {
        KeyCode::Char('q') => app.should_quit = true,
        KeyCode::Tab => {
            app.current_window = match app.current_window {
                TimeWindow::Last24h => TimeWindow::Last7d,
                TimeWindow::Last7d => TimeWindow::Last24h,
            };
            app.selected_issue_index = 0;
        }
        KeyCode::Left => {
            if app.selected_workflow_index > 0 {
                app.selected_workflow_index -= 1;
                app.selected_issue_index = 0;
            }
        }
        KeyCode::Right => {
            if app.selected_workflow_index + 1 < app.workflows.len() {
                app.selected_workflow_index += 1;
                app.selected_issue_index = 0;
            }
        }
        KeyCode::Up => {
            if app.selected_issue_index > 0 {
                app.selected_issue_index -= 1;
            }
        }
        KeyCode::Char('r') => {
            app.needs_refresh = true;
        }
        KeyCode::Down => {
            let len = app.visible_issues().len();
            if app.selected_issue_index + 1 < len {
                app.selected_issue_index += 1;
            }
        }
        _ => {}
    }
}

async fn build_app_state(workflow_config: &EnvVariables) -> Result<AppState> {
    let workflows = load_workflows(workflow_config).await?;

    Ok(AppState {
        workflows,
        selected_workflow_index: 0,
        selected_issue_index: 0,
        current_window: TimeWindow::Last24h,
        last_refresh_ms: Some(now_milli_seconds()),
        should_quit: false,
        needs_refresh: false,
    })
}


fn setup_terminal() -> Result<Terminal<CrosstermBackend<io::Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let terminal = Terminal::new(backend)?;
    Ok(terminal)
}

fn restore_terminal(
    mut terminal: Terminal<CrosstermBackend<io::Stdout>>,
) -> Result<()> {
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}


async fn load_workflows(env_variables: &EnvVariables) -> Result<Vec<WorkflowIssues>> {
    let mut workflows = Vec::new();

    for job_id in &env_variables.job_ids {
        let workflow_name = get_job_name(env_variables, job_id).await?;
        let issues = get_issues(env_variables, job_id).await?;

        workflows.push(WorkflowIssues {
            workflow_name,
            issues,
        });
    }

    Ok(workflows)
}


async fn get_issues(workflow_config : &EnvVariables, job_id: &str ) -> Result<Vec<Issue>>  {
    let job_runs = get_job_info(workflow_config, job_id).await?;
    let failed_runs: Vec<&JobRun> = job_runs.runs.iter().filter(|r| is_failed(r)).collect();
    let issues: Vec<Issue> = failed_runs.into_iter().map(Issue::from_job_run).collect();
    Ok(issues)
}



async fn get_job_info(env_variables: &EnvVariables, job_id: &str) -> Result<JobRunsResponse> {
    let client = reqwest::Client::new();
    let base_url = env_variables.workspace_url.trim_end_matches('/');
    let url = format!("{base_url}/api/2.2/jobs/runs/list?job_id={}", job_id);
    let response = client.get(&url)
        .header(AUTHORIZATION,format!("Bearer {}", env_variables.pat))
        .header(USER_AGENT,"rust-reqwest-client")
        .send().await?;
    if response.status().is_success() {
        let jobs_run: JobRunsResponse = response.json().await?;
        Ok(jobs_run)
    } else {
        let status = response.status();
        let body = response.text().await?;
        Err(anyhow!("Failed to fetch job runs: {status}, body: {body}"))
    }
}
fn draw(frame: &mut ratatui::Frame, app: &AppState) {
    let vertical = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(3),
        Constraint::Length(3),
        Constraint::Min(0),
        Constraint::Length(1),
    ])
        .split(frame.area());

    let body = Layout::horizontal([
        Constraint::Percentage(45),
        Constraint::Percentage(55),
    ])
        .split(vertical[3]);

    draw_header(frame, vertical[0], app);
    draw_workflow_tabs(frame, vertical[1], app);
    draw_time_window_tabs(frame, vertical[2], app);
    draw_issue_list(frame, body[0], app);
    draw_issue_details(frame, body[1], app);
    draw_footer(frame, vertical[4]);
}
fn draw_footer(frame: &mut ratatui::Frame, area: ratatui::layout::Rect) {
    let text = "q quit | r refresh | ← → workflow | tab 24h/7d | ↑ ↓ issue | auto-refresh";
    frame.render_widget(Paragraph::new(text), area);
}
fn draw_issue_list(frame: &mut ratatui::Frame, area: ratatui::layout::Rect, app: &AppState) {
    let visible = app.visible_issues();

    let items: Vec<ListItem> = visible
        .iter()
        .map(|issue| {
            let timestamp = issue
                .start_time_at_ms
                .map(format_timestamp)
                .unwrap_or_else(|| "missing".to_string());

            let line = format!("{timestamp} | {:?} | {}", issue.issue_type, issue.message);
            ListItem::new(Line::from(line))
        })
        .collect();

    let list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title("Issues"))
        .highlight_symbol(">> ");

    let mut state = ListState::default();

    if !visible.is_empty() {
        state.select(Some(app.selected_issue_index));
    }

    frame.render_stateful_widget(list, area, &mut state);
}
fn draw_issue_details(frame: &mut ratatui::Frame, area: ratatui::layout::Rect, app: &AppState) {
    let body = if let Some(issue) = app.selected_issue() {
        format!(
            "Run ID: {}\nWorkflow: {}\nStart: {}\nSeverity: {:?}\nType: {:?}\nRun URL: {}\n\nMessage:\n{}",
            issue.run_id,
            issue.workflow_name.as_deref().unwrap_or("unknown"),
            issue.start_time_at_ms
                .map(format_timestamp)
                .unwrap_or_else(|| "missing".to_string()),
            issue.severity,
            issue.issue_type,
            issue.run_page_url.as_deref().unwrap_or("missing"),
            issue.message,
        )
    } else {
        "No issue selected".to_string()
    };

    let details = Paragraph::new(body)
        .block(Block::default().borders(Borders::ALL).title("Details"));

    frame.render_widget(details, area);
}
fn is_failed(run: &JobRun) -> bool {
    matches!(
        run.state
            .as_ref()
            .and_then(|s| s.result_state.as_deref()),
        Some("FAILED") | Some("TIMEDOUT") | Some("INTERNAL_ERROR")
        )
}

fn is_in_last_24h(issue: &Issue, now: i64) -> bool {
    issue.start_time_at_ms.map_or(false, |t| {
        t >= now - 24 * 60 * 60 * 1000
    })
}

fn is_in_last_7d(issue: &Issue, now: i64) -> bool {
    issue.start_time_at_ms.map_or(false, |t| {
        t >= now - 7 * 24 * 60 * 60 * 1000
    })
}
fn parse_env_variables() -> Result<EnvVariables> {
    let workspace_url = env::var("WORKSPACE_URL").map_err(|_| anyhow!("Env var WORKSPACE_URL not set"))?;
    let pat = env::var("PAT").map_err(|_| anyhow!("Env var PAT not set"))?;
    let job_ids_raw = env::var("JOB_IDS")
        .map_err(|_| anyhow!("Env var JOB_IDS not set"))?;
    let job_ids: Vec<String> = job_ids_raw
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let refresh_rate_minutes = env::var("REFRESH_RATE_MIN")
        .map_err(|_| anyhow!("Env var REFRESH_RATE_MIN not set"))?
        .parse::<i64>()
        .map_err(|_| anyhow!("REFRESH_RATE_MIN must be a valid i64"))?;
    let debug = env::var("DEBUG")
        .unwrap_or_else(|_| "false".to_string())
        .to_lowercase() == "true";
    if job_ids.is_empty() {
        return Err(anyhow!("JOB_IDS must contain at least one job id"));
    }
    Ok(EnvVariables {
        workspace_url,
        pat,
        job_ids,
        refresh_rate_minutes,
        debug,
    })
}



fn now_milli_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_millis() as i64
}

fn format_timestamp (milliseconds: i64) -> String {
    let seconds = milliseconds / 1000;
    let nanoseconds = (milliseconds % 1000) * 1_000_000;
    let date_time = DateTime::<Utc>::from_timestamp(seconds, nanoseconds as u32)
        .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap());
    date_time.format("%Y-%m-%d %H:%M:%S").to_string()
}

async fn get_job_name(env_variables: &EnvVariables, job_id: &str) -> Result<String> {
    let client = reqwest::Client::new();
    let base_url = env_variables.workspace_url.trim_end_matches('/');
    let url = format!("{base_url}/api/2.2/jobs/get?job_id={job_id}");

    let response = client
        .get(&url)
        .header(AUTHORIZATION, format!("Bearer {}", env_variables.pat))
        .header(USER_AGENT, "rust-reqwest-client")
        .send()
        .await?;

    if response.status().is_success() {
        let job: JobResponse = response.json().await?;
        Ok(job.settings.name)
    } else {
        let status = response.status();
        let body = response.text().await?;
        Err(anyhow!("Failed to fetch job metadata: {status}, body: {body}"))
    }
}