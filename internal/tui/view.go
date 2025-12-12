package tui

import (
	"fmt"
	"time"

	"surge/internal/utils"

	"github.com/charmbracelet/lipgloss"
)

// View renders the entire TUI
func (m RootModel) View() string {
	if m.width == 0 {
		return "Loading..."
	}

	if m.state == InputState {
		labelStyle := lipgloss.NewStyle().Width(10).Foreground(ColorSubtext)
		// Centered popup - compact layout
		popup := lipgloss.JoinVertical(lipgloss.Left,
			TitleStyle.Render("Add New Download"),
			"",
			lipgloss.JoinHorizontal(lipgloss.Left, labelStyle.Render("URL:"), m.inputs[0].View()),
			lipgloss.JoinHorizontal(lipgloss.Left, labelStyle.Render("Path:"), m.inputs[1].View()),
			lipgloss.JoinHorizontal(lipgloss.Left, labelStyle.Render("Filename:"), m.inputs[2].View()),
			"",
			lipgloss.NewStyle().Foreground(ColorSubtext).Render("[Enter] Next/Start  [Esc] Cancel"),
		)

		return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center,
			PanelStyle.Padding(1, 2).Render(popup),
		)
	}

	if m.state == DetailState {
		selected := m.downloads[m.cursor]
		details := renderDetails(selected)
		return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center,
			PanelStyle.Padding(1, 2).Render(
				lipgloss.JoinVertical(lipgloss.Left,
					details,
					"",
					lipgloss.NewStyle().Foreground(ColorSubtext).Render("[Esc] Back"),
				),
			),
		)
	}

	// === Header ===
	active, queued, downloaded := m.CalculateStats()
	headerStats := fmt.Sprintf("Active: %d | Queued: %d | Downloaded: %d", active, queued, downloaded)
	header := lipgloss.JoinVertical(lipgloss.Left,
		HeaderStyle.Width(m.width-HeaderWidthOffset).Render("Surge"),
		StatsStyle.Render(headerStats),
	)

	if len(m.downloads) == 0 {
		return lipgloss.JoinVertical(lipgloss.Left,
			header,
			"",
			lipgloss.Place(m.width, m.height-6, lipgloss.Center, lipgloss.Center,
				lipgloss.JoinVertical(lipgloss.Center,
					"No active downloads.",
					"",
					"[g] Add Download  [q] Quit",
				),
			),
		)
	}

	// === List of Cards ===
	var cards []string
	for i, d := range m.downloads {
		cards = append(cards, renderCard(d, i == m.cursor, m.width-ProgressBarWidthOffset))
	}

	listContent := lipgloss.JoinVertical(lipgloss.Left, cards...)

	return lipgloss.JoinVertical(lipgloss.Left,
		header,
		listContent,
		"",
		lipgloss.NewStyle().Foreground(ColorSubtext).Padding(0, 1).Render("[g] Add  [Enter] Details  [q] Quit"),
	)
}

func renderCard(d *DownloadModel, selected bool, width int) string {
	style := CardStyle.Width(width)
	if selected {
		style = SelectedCardStyle.Width(width)
	}

	// Progress
	pct := 0.0
	if d.Total > 0 {
		pct = float64(d.Downloaded) / float64(d.Total)
	}
	d.progress.Width = width - ProgressBarWidthOffset
	progressBar := d.progress.View()

	// Stats line
	eta := "N/A"
	if d.Speed > 0 && d.Total > 0 {
		remainingBytes := d.Total - d.Downloaded
		remainingSeconds := float64(remainingBytes) / d.Speed
		eta = time.Duration(remainingSeconds * float64(time.Second)).Round(time.Second).String()
	}

	stats := fmt.Sprintf("Speed: %.1f MB/s | ETA: %s | %.0f%%", d.Speed/Megabyte, eta, pct*100)
	if d.done {
		stats = fmt.Sprintf("Completed | Size: %s", utils.ConvertBytesToHumanReadable(d.Total))
	}

	content := lipgloss.JoinVertical(lipgloss.Left,
		CardTitleStyle.Render(d.Filename),
		progressBar,
		CardStatsStyle.Render(stats),
	)

	return style.Render(content)
}

func renderDetails(m *DownloadModel) string {
	title := TitleStyle.Render(m.Filename)

	if m.err != nil {
		return lipgloss.JoinVertical(lipgloss.Left,
			title,
			"",
			lipgloss.NewStyle().Foreground(ColorError).Render(fmt.Sprintf("Error: %v", m.err)),
		)
	}

	// Calculate stats
	percentage := 0.0
	if m.Total > 0 {
		percentage = float64(m.Downloaded) / float64(m.Total)
	}

	eta := "N/A"
	if m.Speed > 0 && m.Total > 0 {
		remainingBytes := m.Total - m.Downloaded
		remainingSeconds := float64(remainingBytes) / m.Speed
		eta = time.Duration(remainingSeconds * float64(time.Second)).Round(time.Second).String()
	}

	// Progress Bar
	m.progress.Width = 60
	progressBar := m.progress.View()

	stats := lipgloss.JoinVertical(lipgloss.Left,
		fmt.Sprintf("Progress:    %.1f%%", percentage*100),
		fmt.Sprintf("Size:        %s / %s", utils.ConvertBytesToHumanReadable(m.Downloaded), utils.ConvertBytesToHumanReadable(m.Total)),
		fmt.Sprintf("Speed:       %.1f MB/s", m.Speed/Megabyte),
		fmt.Sprintf("ETA:         %s", eta),
		fmt.Sprintf("Connections: %d", m.Connections),
		fmt.Sprintf("Elapsed:     %s", m.Elapsed.Round(time.Second)),
		fmt.Sprintf("URL:         %s", m.URL),
	)

	return lipgloss.JoinVertical(lipgloss.Left,
		title,
		"",
		progressBar,
		"",
		stats,
	)
}

func (m RootModel) calcTotalSpeed() float64 {
	total := 0.0
	for _, d := range m.downloads {
		total += d.Speed
	}
	return total / Megabyte
}

func (m RootModel) CalculateStats() (active, queued, downloaded int) {
	for _, d := range m.downloads {
		if d.done {
			downloaded++
		} else if d.Speed > 0 {
			active++
		} else {
			queued++
		}
	}
	return
}
