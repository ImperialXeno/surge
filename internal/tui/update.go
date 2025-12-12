package tui

import (
	"context"
	"time"

	"surge/internal/downloader"
	"surge/internal/messages"

	"github.com/charmbracelet/bubbles/progress"
	tea "github.com/charmbracelet/bubbletea"
)

// Update handles messages and updates the model
func (m RootModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case messages.DownloadStartedMsg:
		// Check if download already exists (by ID)
		var target *DownloadModel
		for _, d := range m.downloads {
			if d.ID == msg.DownloadID {
				target = d
				break
			}
		}
		if target != nil {
			// Update existing download with real metadata
			target.Filename = msg.Filename
			target.Total = msg.Total
			target.URL = msg.URL
		} else {
			// Should not happen if we optimistically added it, but fallback just in case
			newDownload := NewDownloadModel(msg.DownloadID, msg.URL, msg.Filename, msg.Total)
			m.downloads = append(m.downloads, newDownload)
		}
		cmds = append(cmds, listenForActivity(m.progressChan))

	case messages.ProgressMsg:
		for _, d := range m.downloads {
			if d.ID == msg.DownloadID {
				d.Downloaded = msg.Downloaded
				d.Speed = msg.Speed
				d.Elapsed = time.Since(d.StartTime)
				d.Connections = msg.ActiveConnections

				if d.Total > 0 {
					percentage := float64(d.Downloaded) / float64(d.Total)
					cmd := d.progress.SetPercent(percentage)
					cmds = append(cmds, cmd)
				}
				break
			}
		}
		cmds = append(cmds, listenForActivity(m.progressChan))

	case messages.DownloadCompleteMsg:
		for _, d := range m.downloads {
			if d.ID == msg.DownloadID {
				d.Downloaded = d.Total // Ensure we show 100%
				d.Elapsed = msg.Elapsed
				d.done = true
				break
			}
		}
		cmds = append(cmds, listenForActivity(m.progressChan))

	case messages.DownloadErrorMsg:
		for _, d := range m.downloads {
			if d.ID == msg.DownloadID {
				d.err = msg.Err
				d.done = true
				break
			}
		}
		cmds = append(cmds, listenForActivity(m.progressChan))

	case messages.TickMsg:
		cmds = append(cmds, tickCmd())

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		// Re-focus inputs to trigger resize if needed (though inputs don't strictly need it here)
		return m, nil

	case tea.KeyMsg:
		switch m.state {
		case DashboardState:
			if msg.String() == "q" || msg.String() == "ctrl+c" {
				return m, tea.Quit
			}
			if msg.String() == "g" {
				m.state = InputState
				m.focusedInput = 0
				m.inputs[0].SetValue("")
				m.inputs[0].Focus()
				m.inputs[1].SetValue(".")
				m.inputs[1].Blur()
				m.inputs[2].SetValue("")
				m.inputs[2].Blur()
				return m, nil
			}

			// Navigation
			if msg.String() == "up" || msg.String() == "k" {
				if m.cursor > 0 {
					m.cursor--
				}
			}
			if msg.String() == "down" || msg.String() == "j" {
				if m.cursor < len(m.downloads)-1 {
					m.cursor++
				}
			}

			// Details
			if msg.String() == "enter" {
				if len(m.downloads) > 0 {
					m.state = DetailState
				}
			}

		case DetailState:
			if msg.String() == "esc" || msg.String() == "q" || msg.String() == "enter" {
				m.state = DashboardState
				return m, nil
			}

		case InputState:
			if msg.String() == "esc" {
				m.state = DashboardState
				return m, nil
			}
			if msg.String() == "enter" {
				// Navigate through inputs: URL -> Path -> Filename -> Start
				if m.focusedInput < 2 {
					m.inputs[m.focusedInput].Blur()
					m.focusedInput++
					m.inputs[m.focusedInput].Focus()
					return m, nil
				}
				// Start download (on last input)
				url := m.inputs[0].Value()
				if url == "" {
					// URL is mandatory - don't start
					m.focusedInput = 0
					m.inputs[0].Focus()
					m.inputs[1].Blur()
					m.inputs[2].Blur()
					return m, nil
				}
				path := m.inputs[1].Value()
				if path == "" {
					path = "."
				}
				// filename := m.inputs[2].Value() // Will use later
				m.state = DashboardState

				// Optimistically add download
				nextID := len(m.downloads) + 1
				newDownload := NewDownloadModel(nextID, url, "Resolving...", 0)
				m.downloads = append(m.downloads, newDownload)

				return m, StartDownloadCmd(m.progressChan, nextID, url, path)
			}

			// Up/Down navigation between inputs
			if msg.String() == "up" && m.focusedInput > 0 {
				m.inputs[m.focusedInput].Blur()
				m.focusedInput--
				m.inputs[m.focusedInput].Focus()
				return m, nil
			}
			if msg.String() == "down" && m.focusedInput < 2 {
				m.inputs[m.focusedInput].Blur()
				m.focusedInput++
				m.inputs[m.focusedInput].Focus()
				return m, nil
			}

			var cmd tea.Cmd
			m.inputs[m.focusedInput], cmd = m.inputs[m.focusedInput].Update(msg)
			return m, cmd
		}
	}

	// Propagate messages to progress bars
	for i := range m.downloads {
		var cmd tea.Cmd
		var newModel tea.Model
		newModel, cmd = m.downloads[i].progress.Update(msg)
		if p, ok := newModel.(progress.Model); ok {
			m.downloads[i].progress = p
		}
		cmds = append(cmds, cmd)
	}

	return m, tea.Batch(cmds...)
}

func StartDownloadCmd(sub chan tea.Msg, id int, url, path string) tea.Cmd {
	return func() tea.Msg {
		d := downloader.NewDownloader()
		d.SetProgressChan(sub)
		d.SetID(id)

		ctx := context.Background()

		go func() {
			err := d.Download(ctx, url, path, 1, false, "", "") // Concurrency restricted to 1 as per user request
			if err != nil {
				sub <- messages.DownloadErrorMsg{DownloadID: id, Err: err}
			}
		}()

		return nil
	}
}
