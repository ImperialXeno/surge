package tui

import (
	"fmt"
	"strings"
	"time"

	"surge/internal/utils"

	"github.com/charmbracelet/lipgloss"
)

// Define the Layout Ratios
const (
	ListWidthRatio = 0.6 // List takes 60% width
)

func (m RootModel) View() string {
	if m.width == 0 {
		return "Loading..."
	}

	// === Handle Modal States First ===
	// These overlays sit on top of the dashboard or replace it

	if m.state == InputState {
		labelStyle := lipgloss.NewStyle().Width(10).Foreground(ColorLightGray)
		// Centered popup - compact layout
		hintStyle := lipgloss.NewStyle().MarginLeft(1).Foreground(ColorLightGray) // Secondary
		if m.focusedInput == 1 {
			hintStyle = lipgloss.NewStyle().MarginLeft(1).Foreground(ColorNeonPink) // Highlighted
		}
		pathLine := lipgloss.JoinHorizontal(lipgloss.Left,
			labelStyle.Render("Path:"),
			m.inputs[1].View(),
			hintStyle.Render("[Tab] Browse"),
		)

		popup := lipgloss.JoinVertical(lipgloss.Left,
			TitleStyle.Render("ADD DOWNLOAD"),
			"",
			lipgloss.JoinHorizontal(lipgloss.Left, labelStyle.Render("URL:"), m.inputs[0].View()),
			pathLine,
			lipgloss.JoinHorizontal(lipgloss.Left, labelStyle.Render("Filename:"), m.inputs[2].View()),
			"",
			lipgloss.NewStyle().Foreground(ColorLightGray).Render("[Enter] Start  [Esc] Cancel"),
		)

		return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center,
			PaneStyle.Width(60).Padding(1, 2).Render(popup),
		)
	}

	if m.state == FilePickerState {
		pickerContent := lipgloss.JoinVertical(lipgloss.Left,
			TitleStyle.Render("SELECT DIRECTORY"),
			"",
			lipgloss.NewStyle().Foreground(ColorLightGray).Render(m.filepicker.CurrentDirectory),
			"",
			m.filepicker.View(),
			"",
			lipgloss.NewStyle().Foreground(ColorLightGray).Render("[.] Select Here  [H] Downloads  [Enter] Open  [Esc] Cancel"),
		)

		return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center,
			PaneStyle.Width(60).Padding(1, 2).Render(pickerContent),
		)
	}

	if m.state == DuplicateWarningState {
		warningContent := lipgloss.JoinVertical(lipgloss.Center,
			lipgloss.NewStyle().Foreground(ColorNeonPink).Bold(true).Render("⚠ DUPLICATE DETECTED"),
			"",
			lipgloss.NewStyle().Foreground(ColorNeonPurple).Bold(true).Render(truncateString(m.duplicateInfo, 50)),
			"",
			lipgloss.NewStyle().Foreground(ColorLightGray).Render("[C] Continue  [F] Focus Existing  [X] Cancel"),
		)

		return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center,
			lipgloss.NewStyle().
				Border(lipgloss.DoubleBorder()).
				BorderForeground(ColorNeonPink).
				Padding(1, 3).
				Render(warningContent),
		)
	}

	// === MAIN DASHBOARD LAYOUT ===

	availableHeight := m.height - 2 // Margin
	availableWidth := m.width - 4   // Margin

	// Top Row Height (Logo + Graph)
	topHeight := 9

	// Bottom Row Height (List + Details)
	bottomHeight := availableHeight - topHeight - 1
	if bottomHeight < 10 {
		bottomHeight = 10
	} // Min height

	// Column Widths
	leftWidth := int(float64(availableWidth) * ListWidthRatio)
	rightWidth := availableWidth - leftWidth - 2 // -2 for spacing

	// --- SECTION 1: HEADER & LOGO (Top Left) ---
	logoText := `
 ██████  ██    ██ ██████   ██████  ███████ 
██       ██    ██ ██   ██ ██       ██      
███████  ██    ██ ██████  ██   ███ █████   
     ██  ██    ██ ██   ██ ██    ██ ██      
███████   ██████  ██   ██  ██████  ███████`

	// Create the header stats
	active, queued, downloaded := m.CalculateStats()
	statsText := fmt.Sprintf("Active: %d  •  Queued: %d  •  Done: %d", active, queued, downloaded)

	headerContent := lipgloss.JoinVertical(lipgloss.Left,
		LogoStyle.Render(logoText),
		lipgloss.NewStyle().Foreground(ColorLightGray).Render(statsText),
	)

	// Use PaneStyle for consistent borders with the graph box
	headerBox := PaneStyle.
		Width(leftWidth).
		Height(topHeight).
		Render(headerContent)

	// --- SECTION 2: SPEED GRAPH (Top Right) ---
	// Render the Sparkline (account for borders: 2 for left/right border, 2 for padding)
	graphContentWidth := rightWidth - 4
	if graphContentWidth < 10 {
		graphContentWidth = 10
	}
	graphContentHeight := topHeight - 4
	if graphContentHeight < 2 {
		graphContentHeight = 2
	}
	graphContent := renderSparkline(m.SpeedHistory, graphContentWidth, graphContentHeight)

	// Get current speed
	currentSpeed := 0.0
	if len(m.SpeedHistory) > 0 {
		currentSpeed = m.SpeedHistory[len(m.SpeedHistory)-1]
	}
	currentSpeedStr := fmt.Sprintf("%.2f MB/s", currentSpeed)

	graphBox := GraphStyle.
		Width(rightWidth).
		Height(topHeight).
		Render(lipgloss.JoinVertical(lipgloss.Right,
			graphContent,
			lipgloss.NewStyle().Foreground(ColorNeonPink).Bold(true).Render(currentSpeedStr),
		))

	// --- SECTION 3: DOWNLOAD LIST (Bottom Left) ---
	// Tab Bar
	tabBar := renderTabs(m.activeTab)

	// Render the bubbles list or centered empty message
	var listContent string
	if len(m.list.Items()) == 0 {
		// Center "No downloads" like the right pane
		listContentHeight := bottomHeight - 6 // Account for tab bar and borders
		listContent = lipgloss.Place(leftWidth-4, listContentHeight, lipgloss.Center, lipgloss.Center,
			lipgloss.NewStyle().Foreground(ColorNeonCyan).Render("No downloads"))
	} else {
		listContent = m.list.View()
	}

	listInner := lipgloss.JoinVertical(lipgloss.Left,
		tabBar,
		listContent,
	)
	listBox := renderBtopBox("downloads", listInner, leftWidth, bottomHeight, ColorNeonPink)

	// --- SECTION 4: DETAILS PANE (Bottom Right) ---
	var detailContent string
	if d := m.GetSelectedDownload(); d != nil {
		detailContent = renderFocusedDetails(d, rightWidth-4)
	} else {
		detailContent = lipgloss.Place(rightWidth-4, bottomHeight-4, lipgloss.Center, lipgloss.Center,
			lipgloss.NewStyle().Foreground(ColorNeonCyan).Render("No Download Selected"))
	}

	detailBox := renderBtopBox("file details", detailContent, rightWidth, bottomHeight, ColorGray)

	// --- ASSEMBLY ---

	// Top Row
	topRow := lipgloss.JoinHorizontal(lipgloss.Top, headerBox, graphBox)

	// Bottom Row
	bottomRow := lipgloss.JoinHorizontal(lipgloss.Top, listBox, detailBox)

	// Footer - show notification if active, otherwise show keybindings
	var footer string
	if m.notification != "" {
		footer = lipgloss.Place(m.width, 1, lipgloss.Center, lipgloss.Center,
			NotificationStyle.Render(m.notification))
	} else {
		footer = lipgloss.NewStyle().Foreground(ColorLightGray).Padding(0, 1).Render(" [Q/W/E] Tabs  [A] Add  [P] Pause  [X] Delete  [/] Filter  [Ctrl+Q] Quit")
	}

	return lipgloss.JoinVertical(lipgloss.Left,
		topRow,
		bottomRow,
		footer,
	)
}

// Helper to render the detailed info pane
func renderFocusedDetails(d *DownloadModel, w int) string {
	pct := 0.0
	if d.Total > 0 {
		pct = float64(d.Downloaded) / float64(d.Total)
	}

	// Progress bar with margins
	progressWidth := w - 12
	if progressWidth < 20 {
		progressWidth = 20
	}
	d.progress.Width = progressWidth
	progView := d.progress.ViewAs(pct)
	pctStr := fmt.Sprintf("%.0f%%", pct*100)

	// Consistent content width for centering
	contentWidth := w - 6

	// Section divider
	divider := lipgloss.NewStyle().
		Foreground(ColorGray).
		Render(strings.Repeat("─", contentWidth))

	// File info section
	fileInfo := lipgloss.JoinVertical(lipgloss.Left,
		lipgloss.JoinHorizontal(lipgloss.Left, StatsLabelStyle.Render("Filename:"), StatsValueStyle.Render(truncateString(d.Filename, contentWidth-14))),
		"",
		lipgloss.JoinHorizontal(lipgloss.Left, StatsLabelStyle.Render("Status:"), StatsValueStyle.Render(getDownloadStatus(d))),
		lipgloss.JoinHorizontal(lipgloss.Left, StatsLabelStyle.Render("Size:"), StatsValueStyle.Render(fmt.Sprintf("%s / %s", utils.ConvertBytesToHumanReadable(d.Downloaded), utils.ConvertBytesToHumanReadable(d.Total)))),
	)

	// Progress section with percentage aligned right
	progressLabel := lipgloss.NewStyle().
		Foreground(ColorNeonCyan).
		Bold(true).
		Render("PROGRESS")
	progressPct := lipgloss.NewStyle().
		Foreground(ColorNeonPink).
		Bold(true).
		Render(pctStr)
	progressHeader := lipgloss.JoinHorizontal(lipgloss.Top,
		progressLabel,
		lipgloss.NewStyle().Width(contentWidth-lipgloss.Width(progressLabel)-lipgloss.Width(progressPct)).Render(""),
		progressPct,
	)
	progressSection := lipgloss.JoinVertical(lipgloss.Left,
		progressHeader,
		"",
		lipgloss.NewStyle().MarginLeft(1).Render(progView),
	)

	// Stats section
	statsSection := lipgloss.JoinVertical(lipgloss.Left,
		lipgloss.JoinHorizontal(lipgloss.Left, StatsLabelStyle.Render("Speed:"), StatsValueStyle.Render(fmt.Sprintf("%.2f MB/s", d.Speed/Megabyte))),
		lipgloss.JoinHorizontal(lipgloss.Left, StatsLabelStyle.Render("Conns:"), StatsValueStyle.Render(fmt.Sprintf("%d", d.Connections))),
		lipgloss.JoinHorizontal(lipgloss.Left, StatsLabelStyle.Render("Elapsed:"), StatsValueStyle.Render(d.Elapsed.Round(time.Second).String())),
	)

	// URL section
	urlSection := lipgloss.JoinVertical(lipgloss.Left,
		lipgloss.JoinHorizontal(lipgloss.Left, StatsLabelStyle.Render("URL:"), lipgloss.NewStyle().Foreground(ColorLightGray).Render(truncateString(d.URL, contentWidth-14))),
	)

	// Combine all sections with dividers and spacing
	content := lipgloss.JoinVertical(lipgloss.Left,
		fileInfo,
		"",
		divider,
		"",
		progressSection,
		"",
		divider,
		"",
		statsSection,
		"",
		divider,
		"",
		urlSection,
	)

	// Wrap in a container with margins
	return lipgloss.NewStyle().
		Padding(1, 2).
		Render(content)
}

func getDownloadStatus(d *DownloadModel) string {
	if d.err != nil {
		return "Error"
	}
	if d.paused {
		return "Paused"
	}
	if d.done {
		return "Completed"
	}
	if d.Speed == 0 && d.Downloaded == 0 {
		return "Queued"
	}
	return "Downloading"
}

// Simple Sparkline Generator using Braille patterns
func renderSparkline(data []float64, w, h int) string {
	if len(data) == 0 {
		return ""
	}

	// Find max for scaling
	max := 0.0
	for _, v := range data {
		if v > max {
			max = v
		}
	}
	if max == 0 {
		max = 1
	}

	// Braille characters
	// distinct levels: ' ', '⡀', '⣀', '⣄', '⣤', '⣦', '⣶', '⣷', '⣿'
	levels := []rune{' ', '⡀', '⣀', '⣄', '⣤', '⣦', '⣶', '⣷', '⣿'}

	// Sample the data to fit width
	// We want to show the latest data at the right
	// If we have more pixels (w) than data, we stretch? No, sparklines usually just show available data.

	// Actually, let's just map data points to character columns.
	// We have 40 history points, width might be ~60 chars.

	var s strings.Builder

	// Ensure we don't go out of bounds
	startIndex := 0
	if len(data) > w {
		startIndex = len(data) - w
	}

	visibleData := data[startIndex:]

	for _, val := range visibleData {
		levelIdx := int((val / max) * float64(len(levels)-1))
		if levelIdx < 0 {
			levelIdx = 0
		}
		if levelIdx >= len(levels) {
			levelIdx = len(levels) - 1
		}
		s.WriteRune(levels[levelIdx])
	}
	// Fill remaining width if any (pad left)
	// But usually we just return what we have

	return lipgloss.NewStyle().Foreground(ColorNeonPink).Render(s.String())
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

func truncateString(s string, i int) string {
	runes := []rune(s)
	if len(runes) > i {
		return string(runes[:i]) + "..."
	}
	return s
}

func renderTabs(active int) string {
	tabs := []string{"Queued", "Active", "Done"}
	var rendered []string
	for i, t := range tabs {
		var style lipgloss.Style
		if i == active {
			style = ActiveTabStyle
		} else {
			style = TabStyle
		}
		rendered = append(rendered, style.Render(t))
	}
	return lipgloss.JoinHorizontal(lipgloss.Top, rendered...)
}

// renderBtopBox creates a btop-style box with title embedded in the top border
// Example: ╭─ title ─────────────────────────────────╮
func renderBtopBox(title string, content string, width, height int, borderColor lipgloss.Color) string {
	// Border characters
	const (
		topLeft     = "╭"
		topRight    = "╮"
		bottomLeft  = "╰"
		bottomRight = "╯"
		horizontal  = "─"
		vertical    = "│"
	)

	innerWidth := width - 2 // Account for left and right borders
	if innerWidth < 1 {
		innerWidth = 1
	}

	// Build top border with embedded title: ╭─ title ──────────╮
	titleText := fmt.Sprintf(" %s ", title)
	titleLen := len(titleText)
	remainingWidth := innerWidth - titleLen - 1 // -1 for the dash after topLeft
	if remainingWidth < 0 {
		remainingWidth = 0
	}

	topBorder := topLeft + horizontal +
		lipgloss.NewStyle().Foreground(ColorNeonCyan).Bold(true).Render(titleText) +
		lipgloss.NewStyle().Foreground(borderColor).Render(strings.Repeat(horizontal, remainingWidth)) +
		lipgloss.NewStyle().Foreground(borderColor).Render(topRight)

	// Build bottom border: ╰───────────────────╯
	bottomBorder := lipgloss.NewStyle().Foreground(borderColor).Render(
		bottomLeft + strings.Repeat(horizontal, innerWidth) + bottomRight,
	)

	// Style for vertical borders
	borderStyle := lipgloss.NewStyle().Foreground(borderColor)

	// Wrap content lines with vertical borders
	contentLines := strings.Split(content, "\n")
	innerHeight := height - 2 // Account for top and bottom borders

	var wrappedLines []string
	for i := 0; i < innerHeight; i++ {
		var line string
		if i < len(contentLines) {
			line = contentLines[i]
		} else {
			line = ""
		}
		// Pad or truncate line to fit innerWidth
		lineWidth := lipgloss.Width(line)
		if lineWidth < innerWidth {
			line = line + strings.Repeat(" ", innerWidth-lineWidth)
		} else if lineWidth > innerWidth {
			// Truncate (simplified - just take first innerWidth chars)
			runes := []rune(line)
			if len(runes) > innerWidth {
				line = string(runes[:innerWidth])
			}
		}
		wrappedLines = append(wrappedLines, borderStyle.Render(vertical)+line+borderStyle.Render(vertical))
	}

	// Combine all parts
	return lipgloss.JoinVertical(lipgloss.Left,
		topBorder,
		strings.Join(wrappedLines, "\n"),
		bottomBorder,
	)
}
