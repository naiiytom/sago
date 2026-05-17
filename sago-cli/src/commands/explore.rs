use anyhow::{Context, Result};
use ratatui::Frame;
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind};
use ratatui::crossterm::execute;
use ratatui::crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Paragraph};
use sago_core::config::Config;
use sago_core::state::ProjectState;
use std::path::Path;

pub fn run() -> Result<()> {
    let toml_str = std::fs::read_to_string("Sago.toml")
        .context("Sago.toml not found — run `sago init` first")?;
    let _cfg = Config::from_toml(&toml_str)?;
    let state = ProjectState::load_or_default(Path::new(".sago/state.json"))?;
    let target_names: Vec<String> = state.snapshots.keys().cloned().collect();
    if target_names.is_empty() {
        anyhow::bail!("no snapshots found — run `sago apply` first");
    }
    run_tui(target_names)
}

#[derive(Debug, PartialEq)]
pub enum View {
    List,
    Detail(usize),
}

pub struct App {
    pub targets: Vec<String>,
    pub selected: usize,
    pub view: View,
    pub should_quit: bool,
}

impl App {
    pub fn new(targets: Vec<String>) -> Self {
        Self {
            targets,
            selected: 0,
            view: View::List,
            should_quit: false,
        }
    }

    pub fn handle_key(&mut self, key: KeyCode) {
        match &self.view {
            View::List => match key {
                KeyCode::Char('q') | KeyCode::Esc => self.should_quit = true,
                KeyCode::Down => {
                    if !self.targets.is_empty() {
                        self.selected = (self.selected + 1) % self.targets.len();
                    }
                }
                KeyCode::Up => {
                    if !self.targets.is_empty() {
                        self.selected = self
                            .selected
                            .checked_sub(1)
                            .unwrap_or(self.targets.len() - 1);
                    }
                }
                KeyCode::Enter => self.view = View::Detail(self.selected),
                _ => {}
            },
            View::Detail(_) => match key {
                KeyCode::Char('q') => self.should_quit = true,
                KeyCode::Esc | KeyCode::Backspace => self.view = View::List,
                _ => {}
            },
        }
    }
}

fn run_tui(targets: Vec<String>) -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = ratatui::backend::CrosstermBackend::new(stdout);
    let mut terminal = ratatui::Terminal::new(backend)?;
    let mut app = App::new(targets);
    let result = run_app(&mut terminal, &mut app);
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    result
}

fn run_app<B: ratatui::backend::Backend>(
    terminal: &mut ratatui::Terminal<B>,
    app: &mut App,
) -> Result<()> {
    loop {
        terminal.draw(|f| render(f, app))?;
        if let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
        {
            app.handle_key(key.code);
        }
        if app.should_quit {
            break;
        }
    }
    Ok(())
}

fn render(f: &mut Frame, app: &App) {
    match &app.view {
        View::List => render_list_view(f, app),
        View::Detail(idx) => render_detail_view(f, app, *idx),
    }
}

pub fn render_list_view(f: &mut Frame, app: &App) {
    let area = f.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(3), Constraint::Length(1)])
        .split(area);
    let items: Vec<ListItem> = app
        .targets
        .iter()
        .map(|name| ListItem::new(name.as_str()))
        .collect();
    let mut state = ListState::default();
    state.select(Some(app.selected));
    let list = List::new(items)
        .block(
            Block::default()
                .title("Sago Explorer")
                .borders(Borders::ALL),
        )
        .highlight_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("> ");
    f.render_stateful_widget(list, chunks[0], &mut state);
    let help = Paragraph::new(Line::from(vec![
        Span::raw("↑↓ navigate  "),
        Span::raw("Enter detail  "),
        Span::raw("q quit"),
    ]));
    f.render_widget(help, chunks[1]);
}

pub fn render_detail_view(f: &mut Frame, app: &App, idx: usize) {
    let area = f.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(3), Constraint::Length(1)])
        .split(area);
    let name = app.targets.get(idx).map(String::as_str).unwrap_or("?");
    let text = format!("Target: {name}\n\nRun `sago plan` to see drift details.");
    let paragraph = Paragraph::new(text).block(Block::default().title(name).borders(Borders::ALL));
    f.render_widget(paragraph, chunks[0]);
    let help = Paragraph::new("Esc back  q quit");
    f.render_widget(help, chunks[1]);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_app(targets: Vec<&str>) -> App {
        App::new(targets.into_iter().map(String::from).collect())
    }

    #[test]
    fn test_app_initial_state() {
        let app = make_app(vec!["users", "events"]);
        assert_eq!(app.selected, 0);
        assert!(matches!(app.view, View::List));
        assert!(!app.should_quit);
    }

    #[test]
    fn test_navigate_down_wraps() {
        let mut app = make_app(vec!["a", "b", "c"]);
        app.handle_key(KeyCode::Down);
        assert_eq!(app.selected, 1);
        app.handle_key(KeyCode::Down);
        assert_eq!(app.selected, 2);
        app.handle_key(KeyCode::Down);
        assert_eq!(app.selected, 0);
    }

    #[test]
    fn test_navigate_up_wraps() {
        let mut app = make_app(vec!["a", "b", "c"]);
        app.handle_key(KeyCode::Up);
        assert_eq!(app.selected, 2);
    }

    #[test]
    fn test_enter_switches_to_detail() {
        let mut app = make_app(vec!["users"]);
        app.handle_key(KeyCode::Enter);
        assert!(matches!(app.view, View::Detail(_)));
    }

    #[test]
    fn test_esc_returns_to_list() {
        let mut app = make_app(vec!["users"]);
        app.handle_key(KeyCode::Enter);
        app.handle_key(KeyCode::Esc);
        assert!(matches!(app.view, View::List));
    }

    #[test]
    fn test_q_quits() {
        let mut app = make_app(vec!["users"]);
        app.handle_key(KeyCode::Char('q'));
        assert!(app.should_quit);
    }

    #[test]
    fn test_list_view_renders_target_names() {
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;
        let backend = TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        let app = make_app(vec!["users", "events_2024"]);
        terminal.draw(|f| render_list_view(f, &app)).unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer
            .content()
            .iter()
            .map(|c| c.symbol().to_string())
            .collect();
        assert!(content.contains("users"));
        assert!(content.contains("events_2024"));
    }

    #[test]
    fn test_detail_view_renders_target_name() {
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;
        let backend = TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        let mut app = make_app(vec!["users"]);
        app.view = View::Detail(0);
        terminal.draw(|f| render_detail_view(f, &app, 0)).unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer
            .content()
            .iter()
            .map(|c| c.symbol().to_string())
            .collect();
        assert!(content.contains("users"));
    }
}
