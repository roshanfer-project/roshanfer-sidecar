"""Generic academic plotting primitives with ACM compact styling.

This module provides reusable, domain-agnostic plotting tools optimized for
academic papers (specifically ACM format). All styling is consistent and compact.

Design goals:
- Generic primitives (line, stack, bar, scatter) not domain-specific
- Width specified in points (academic paper standard)
- Consistent compact spacing across all plots
- Support for uneven subplot sizing (GridSpec)
- Helper functions for common label patterns
- High-contrast colors optimized for print

Usage:
    >>> style = ACM_COMPACT_HALF  # 3.33" half column
    >>> grid = SubplotGrid(style, layout="row-3")
    >>> plot_line(grid.get_ax(0, 0), x_data, y_data, style=style)
    >>> grid.configure_labels(pattern="leftmost_y_bottom_x", ylabel="Latency (ms)")
    >>> grid.add_shared_legend(position="top")
    >>> grid.save(Path("output.pdf"))
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
import numpy as np


@dataclass
class PlotStyle:
    """ACM paper compact styling configuration.
    
    All spacing and sizing values are consistent across all plots to ensure
    uniform appearance in academic papers.
    
    Attributes:
        width_points: Figure width in points (primary input, converts to inches)
        aspect_ratio: Height/width ratio (default: golden ratio)
        dpi: Resolution for saved figures
        
        Compact spacing (CONSISTENT):
        hspace: Vertical space between subplot rows
        wspace: Horizontal space between subplot columns
        margins: Figure margins (left, right, top, bottom)
        
        Typography:
        font_size: Base font size for axis labels
        title_size: Subplot title font size
        legend_size: Legend font size
        
        Visual elements:
        line_width: Line width for plots
        marker_size: Marker size for scatter/line plots
        bar_width_fraction: Bar width as fraction of x-unit
        bar_spacing_fraction: Bar width as fraction of allocated group space
        
        Colors/styles:
        colors: High-contrast color palette (optimized for print)
        line_styles: Line style variations
        markers: Marker shape variations
    """
    width_points: float  # Input in points (e.g., 240 for ACM half column)
    aspect_ratio: float = 0.618  # Golden ratio
    dpi: int = 300
    
    # Compact spacing - CONSISTENT across all plots
    hspace: float = 0.15  # Vertical space between subplot rows
    wspace: float = 0.08  # Horizontal space between subplot columns
    left_margin: float = 0.10
    right_margin: float = 0.98
    top_margin: float = 0.88
    bottom_margin: float = 0.12
    
    # Font sizes
    font_size: int = 10
    title_size: int = 11
    legend_size: int = 9
    
    # Line/marker styles
    line_width: float = 1.8
    marker_size: float = 4.5
    
    # Bar plot settings
    bar_width_fraction: float = 0.8  # Fraction of x-unit
    bar_spacing_fraction: float = 0.9  # Bar width as fraction of allocated space
    
    # Axis tick configuration
    x_tick_step: Optional[float] = None  # X-axis tick step (auto if None)
    y_tick_step: Optional[float] = None  # Y-axis tick step (auto if None)
    x_tick_type: str = "auto"  # "int", "float", or "auto"
    y_tick_type: str = "auto"  # "int", "float", or "auto"
    axis_guard_fraction: float = 0.03  # Padding fraction for professional appearance (default)
    
    # High-contrast academic colors (optimized for print)
    colors: List[str] = field(default_factory=lambda: [
        '#e41a1c',  # Red (high contrast)
        '#377eb8',  # Blue
        '#4daf4a',  # Green
        '#984ea3',  # Purple
        '#ff7f00',  # Orange
        '#a65628',  # Brown
        '#f781bf',  # Pink
        '#999999',  # Gray
    ])
    
    line_styles: List[str] = field(default_factory=lambda: ['-', '--', '-.', ':'])
    markers: List[str] = field(default_factory=lambda: ['o', 's', '^', 'D', 'v', 'P', '*'])
    
    @property
    def width_inches(self) -> float:
        """Convert points to inches (72 points = 1 inch)."""
        return self.width_points / 72.0


# ACM Presets
ACM_QUARTER = PlotStyle(width_points=120)  # 1.665 inches (quarter column)
ACM_COMPACT_HALF = PlotStyle(width_points=240)  # 3.33 inches (half column)
ACM_COMPACT_FULL = PlotStyle(width_points=504)  # 7 inches (full column)


class SubplotGrid:
    """Flexible multi-panel figure manager with compact ACM styling.
    
    Manages creation and configuration of multi-subplot figures with:
    - Consistent compact spacing
    - Uneven subplot sizing via GridSpec
    - Helper methods for common label patterns
    - Figure-level legend management
    
    Example:
        >>> style = ACM_COMPACT_HALF
        >>> grid = SubplotGrid(style, layout="2x3")  # 2 rows, 3 columns
        >>> ax = grid.get_ax(row=0, col=1)
        >>> grid.configure_labels("leftmost_y_bottom_x", ylabel="Rate (KRPS)")
        >>> grid.add_shared_legend(position="top")
        >>> grid.save(Path("figure.pdf"))
    """
    
    def __init__(self, style: PlotStyle, layout: str = "1x1",
                 width_ratios: Optional[List[float]] = None,
                 height_ratios: Optional[List[float]] = None):
        """Initialize subplot grid.
        
        Args:
            style: PlotStyle configuration
            layout: Layout string - "MxN" (e.g., "2x3") or "row-N" (e.g., "row-3")
            width_ratios: Relative widths for columns (enables uneven subplot widths)
            height_ratios: Relative heights for rows (enables uneven subplot heights)
        """
        self.style = style
        self._parse_layout(layout)
        self._create_figure(width_ratios, height_ratios)
        self._apply_compact_spacing()
    
    def _parse_layout(self, layout: str):
        """Parse layout string into rows and columns."""
        if layout.startswith("row-"):
            self.nrows, self.ncols = 1, int(layout.split("-")[1])
        elif "x" in layout:
            parts = layout.split("x")
            self.nrows, self.ncols = int(parts[0]), int(parts[1])
        else:
            raise ValueError(f"Invalid layout: {layout}. Use 'MxN' or 'row-N' format.")
    
    def _create_figure(self, width_ratios, height_ratios):
        """Create figure with matplotlib, optionally using GridSpec for uneven sizing."""
        import matplotlib.pyplot as plt
        import matplotlib.gridspec as gridspec
        
        total_width = self.style.width_inches * self.ncols
        total_height = self.style.width_inches * self.style.aspect_ratio * self.nrows
        
        self.fig = plt.figure(figsize=(total_width, total_height), dpi=self.style.dpi)
        
        if width_ratios or height_ratios:
            # Use GridSpec for uneven subplot sizing
            gs = gridspec.GridSpec(
                self.nrows, self.ncols,
                width_ratios=width_ratios or [1] * self.ncols,
                height_ratios=height_ratios or [1] * self.nrows,
                figure=self.fig,
                hspace=self.style.hspace,
                wspace=self.style.wspace
            )
            self.axes = []
            for i in range(self.nrows):
                for j in range(self.ncols):
                    ax = self.fig.add_subplot(gs[i, j])
                    ax.tick_params(labelsize=self.style.font_size - 1)
                    self.axes.append(ax)
        else:
            # Regular uniform subplots
            _, axes = plt.subplots(
                self.nrows, self.ncols,
                figsize=(total_width, total_height),
                dpi=self.style.dpi
            )
            if self.nrows * self.ncols == 1:
                self.axes = [axes]
            else:
                self.axes = np.array(axes).flatten().tolist()
            self.fig = plt.gcf()
    
    def _apply_compact_spacing(self):
        """Apply consistent compact spacing to figure."""
        self.fig.subplots_adjust(
            left=self.style.left_margin,
            right=self.style.right_margin,
            top=self.style.top_margin,
            bottom=self.style.bottom_margin,
            hspace=self.style.hspace,
            wspace=self.style.wspace
        )
    
    def get_ax(self, row: int = 0, col: int = 0):
        """Get axis at specified (row, col) position.
        
        Args:
            row: Row index (0-based)
            col: Column index (0-based)
            
        Returns:
            Matplotlib axis object
        """
        idx = row * self.ncols + col
        return self.axes[idx]
    
    def configure_ax(self, ax, xlabel: str = "", ylabel: str = "", title: str = "",
                    show_xlabel: bool = True, show_ylabel: bool = True,
                    show_xticklabels: bool = True, show_yticklabels: bool = True,
                    grid: bool = True, log_y: bool = False, log_x: bool = False,
                    x_data=None, y_data=None, 
                    x_step: Optional[float] = None, y_step: Optional[float] = None,
                    x_type: str = "auto", y_type: str = "auto",
                    x_guard: Optional[float] = None, y_guard: Optional[float] = None,
                    auto_ticks: bool = True,
                    xlim: Optional[Tuple[float, float]] = None,
                    ylim: Optional[Tuple[float, float]] = None):
        """Configure individual axis with common settings.
        
        Args:
            ax: Matplotlib axis to configure
            xlabel: X-axis label text
            ylabel: Y-axis label text
            title: Subplot title
            show_xlabel: Whether to display x-label
            show_ylabel: Whether to display y-label
            show_xticklabels: Whether to display x-tick labels
            show_yticklabels: Whether to display y-tick labels
            grid: Whether to show grid
            log_y: Use logarithmic y-axis
            log_x: Use logarithmic x-axis
            x_data: X-axis data for automatic tick configuration (optional)
            y_data: Y-axis data for automatic tick configuration (optional)
            x_step: X-axis tick step (overrides style default)
            y_step: Y-axis tick step (overrides style default)
            x_type: X-axis tick type - "int", "float", or "auto"
            y_type: Y-axis tick type - "int", "float", or "auto"
            x_guard: X-axis guard fraction (overrides style default)
            y_guard: Y-axis guard fraction (overrides style default)
            auto_ticks: Whether to automatically configure ticks (default: True)
            xlim: X-axis limits as (min, max) tuple (optional, used for both ticks and limits)
            ylim: Y-axis limits as (min, max) tuple (optional, used for both ticks and limits)
        """
        if xlabel and show_xlabel:
            ax.set_xlabel(xlabel, fontsize=self.style.font_size)
        if ylabel and show_ylabel:
            ax.set_ylabel(ylabel, fontsize=self.style.font_size)
        if title:
            ax.set_title(title, fontsize=self.style.title_size, pad=4)
        
        
        if grid:
            ax.grid(True, alpha=0.3, linewidth=0.5)
        
        if log_y:
            ax.set_yscale('log')

            if ylim is not None:
                ax.set_ylim(ylim[0], ylim[1])
        else:
            if auto_ticks and (y_data is not None or ylim is not None):
                configure_y_axis_ticks(ax, y_data=y_data, style=self.style,
                                       y_step=y_step, y_type=y_type,
                                       y_guard=y_guard, ylim=ylim)
                ax.set_ylim(ylim[0], ylim[1])
            elif ylim is not None:
                ax.set_ylim(ylim[0], ylim[1])

        if log_x:
            ax.set_xscale('log')
        
        # Configure ticks automatically if requested and data/range is provided
        if auto_ticks and (x_data is not None or xlim is not None):
            configure_x_axis_ticks(ax, x_data=x_data, style=self.style,
                                   x_step=x_step, x_type=x_type,
                                   x_guard=x_guard, xlim=xlim)
        else:
            if xlim is not None:
                ax.set_xlim(xlim[0], xlim[1])
        
        # Apply tick label visibility settings LAST to override any auto-configuration
        if not show_xticklabels:
            ax.set_xticklabels([])
        if not show_yticklabels:
            ax.set_yticklabels([])
        
       
    def configure_labels(self, pattern: str = "leftmost_y_bottom_x",
                        xlabel: str = "", ylabel: str = ""):
        """Apply common label pattern across all subplots.
        
        Provides convenient presets for typical multi-subplot configurations.
        
        Args:
            pattern: Label pattern to apply:
                - "leftmost_y_bottom_x": Only leftmost column shows y-labels/ticks,
                                         only bottom row shows x-labels/ticks (DEFAULT)
                - "all": All subplots show labels and ticks
                - "none": No labels or ticks on any subplot
            xlabel: X-axis label text (applied based on pattern)
            ylabel: Y-axis label text (applied based on pattern)
        """
        for idx, ax in enumerate(self.axes):
            row = idx // self.ncols
            col = idx % self.ncols
            
            if pattern == "leftmost_y_bottom_x":
                show_ylabel = (col == 0)
                show_xlabel = (row == self.nrows - 1)
                show_yticklabels = (col == 0)
                show_xticklabels = (row == self.nrows - 1)
            elif pattern == "all":
                show_ylabel = show_xlabel = show_yticklabels = show_xticklabels = True
            elif pattern == "none":
                show_ylabel = show_xlabel = show_yticklabels = show_xticklabels = False
            else:
                raise ValueError(f"Unknown pattern: {pattern}")
            
            self.configure_ax(
                ax,
                xlabel=xlabel,
                ylabel=ylabel,
                show_xlabel=show_xlabel,
                show_ylabel=show_ylabel,
                show_xticklabels=show_xticklabels,
                show_yticklabels=show_yticklabels
            )
    
    def add_shared_legend(self, position: str = "top", ncol: Optional[int] = None,
                         handles=None, labels=None, two_rows: bool = False, y_offset: float = 1.1):
        """Add figure-level legend shared across all subplots.
        
        Args:
            position: Legend position ("top" or "bottom")
            ncol: Number of legend columns (auto if None)
            handles: Legend handles (auto-collected from axes if None)
            labels: Legend labels (auto-collected from axes if None)
            two_rows: Force legend into two rows (default: False, uses single row)
        """
        import matplotlib.pyplot as plt
        import math
        
        if handles is None:
            # Collect unique handles/labels from all axes
            all_handles, all_labels = [], []
            for ax in self.axes:
                h, l = ax.get_legend_handles_labels()
                for hh, ll in zip(h, l):
                    if ll not in all_labels:
                        all_handles.append(hh)
                        all_labels.append(ll)
            handles, labels = all_handles, all_labels
        
        if not handles:
            return
        
        # Determine number of columns
        if ncol is None:
            if two_rows:
                # Split into two rows: ceil(n/2) columns per row
                ncol = math.ceil(len(labels) / 2)
            else:
                # Default: single row with all items
                ncol = len(labels)
        
        # Adjust position based on number of rows (two-row needs more space)
        if position == "top":
            # Two-row legends need more vertical clearance to avoid overlapping plot
            bbox = (0.5, y_offset)
            loc = 'upper center'
        elif position == "bottom":
            y_offset = -0.08 if two_rows else -0.05
            bbox = (0.5, y_offset)
            loc = 'lower center'
        else:
            y_offset = 1.10 if two_rows else 1.04
            bbox = (0.5, y_offset)
            loc = 'upper center'
        
        self.fig.legend(
            handles, labels,
            loc=loc, bbox_to_anchor=bbox,
            ncol=ncol, frameon=True, fancybox=True,
            framealpha=0.9, edgecolor='#999999',
            fontsize=self.style.legend_size
        )
    
    def save(self, path: Path):
        """Save figure to file and close.
        
        Args:
            path: Output file path (typically .pdf for academic papers)
        """
        import matplotlib.pyplot as plt
        path.parent.mkdir(parents=True, exist_ok=True)
        self.fig.savefig(path, bbox_inches='tight', dpi=self.style.dpi)
        plt.close(self.fig)


# ============================================================================
# Axis Configuration Helpers
# ============================================================================

def configure_x_axis_ticks(ax, x_data=None, style: Optional[PlotStyle] = None,
                           x_step: Optional[float] = None, x_type: str = "auto",
                           x_guard: Optional[float] = None, xlim: Optional[Tuple[float, float]] = None):
    """Configure x-axis ticks with professional spacing and formatting.
    
    Automatically generates tick positions with proper spacing and guards for
    professional-looking plots. Supports both integer and float formatting.
    """
    
    import math
    
    style = style or ACM_COMPACT_HALF

    # Use provided parameters or fall back to style defaults
    x_step = x_step if x_step is not None else style.x_tick_step
    x_type = x_type if x_type != "auto" else style.x_tick_type
    x_guard = x_guard if x_guard is not None else style.axis_guard_fraction
    
    # Configure X-axis ticks
    # Determine x_min and x_max from either x_data or xlim tuple
    if x_data is not None and len(x_data) > 0:
        data_x_min, data_x_max = float(np.min(x_data)), float(np.max(x_data))
        # Use xlim if provided, otherwise use data range
        if xlim is not None:
            final_x_min, final_x_max = float(xlim[0]), float(xlim[1])
        else:
            final_x_min, final_x_max = data_x_min, data_x_max
    elif xlim is not None:
        final_x_min, final_x_max = float(xlim[0]), float(xlim[1])
    else:
        final_x_min = final_x_max = None
    
    if final_x_min is not None and final_x_max is not None:
        
        # Auto-calculate step if not provided
        if x_step is None:
            x_range = final_x_max - final_x_min
            # Use nice step sizes to get roughly 4-6 ticks
            magnitude = 10 ** math.floor(math.log10(x_range))
            nice_steps = [1, 2, 5, 10]
            
            # Find the step that gives us closest to 5 ticks
            target_ticks = 5
            best_step = nice_steps[0] * magnitude
            best_tick_count = abs(x_range / best_step - target_ticks)
            
            for step_multiplier in nice_steps:
                candidate_step = step_multiplier * magnitude
                tick_count = x_range / candidate_step
                error = abs(tick_count - target_ticks)
                if error < best_tick_count:
                    best_tick_count = error
                    best_step = candidate_step
            
            x_step = best_step
        
        # Generate tick positions
        tick_start = math.floor(final_x_min / x_step) * x_step
        tick_end = math.ceil(final_x_max / x_step) * x_step
        x_ticks = np.arange(tick_start, tick_end + x_step/2, x_step)
        
        # Format tick labels
        if x_type == "int" or (x_type == "auto" and all(abs(t - round(t)) < 1e-9 for t in x_ticks)):
            x_labels = [str(int(t)) for t in x_ticks]
        else:
            x_labels = [f"{t:g}" for t in x_ticks]
        
        ax.set_xticks(x_ticks)
        ax.set_xticklabels(x_labels)
        
        # Set limits with guards (only if xlim was not explicitly provided)
        if xlim is None:
            x_span = final_x_max - final_x_min
            x_pad = x_guard * x_span if x_span > 0 else x_guard * x_step
            ax.set_xlim(final_x_min - x_pad, final_x_max + x_pad)


def configure_y_axis_ticks(ax, y_data=None, style: Optional[PlotStyle] = None,
                           y_step: Optional[float] = None, y_type: str = "auto",
                           y_guard: Optional[float] = None, ylim: Optional[Tuple[float, float]] = None):
    """Configure y-axis ticks with professional spacing and formatting.
    
    Automatically generates tick positions with proper spacing and guards for
    professional-looking plots. Supports both integer and float formatting.
    """
    
    import math
    
    style = style or ACM_COMPACT_HALF
    
    # Use provided parameters or fall back to style defaults
    y_step = y_step if y_step is not None else style.y_tick_step
    y_type = y_type if y_type != "auto" else style.y_tick_type
    y_guard = y_guard if y_guard is not None else style.axis_guard_fraction
    
    # Configure Y-axis ticks
    # Determine y_min and y_max from either y_data or ylim tuple
    if y_data is not None and len(y_data) > 0:
        data_y_min, data_y_max = float(np.min(y_data)), float(np.max(y_data))
        # Use ylim if provided, otherwise use data range
        if ylim is not None:
            final_y_min, final_y_max = float(ylim[0]), float(ylim[1])
        else:
            final_y_min, final_y_max = data_y_min, data_y_max
    elif ylim is not None:
        final_y_min, final_y_max = float(ylim[0]), float(ylim[1])
    else:
        final_y_min = final_y_max = None
    
    if final_y_min is not None and final_y_max is not None:
        
        # Auto-calculate step if not provided
        if y_step is None:
            y_range = final_y_max - final_y_min
            # Use nice step sizes to get roughly 4-6 ticks
            magnitude = 10 ** math.floor(math.log10(y_range))
            nice_steps = [1, 2, 5, 10]
            
            # Find the step that gives us closest to 5 ticks
            target_ticks = 5
            best_step = nice_steps[0] * magnitude
            best_tick_count = abs(y_range / best_step - target_ticks)
            
            for step_multiplier in nice_steps:
                candidate_step = step_multiplier * magnitude
                tick_count = y_range / candidate_step
                error = abs(tick_count - target_ticks)
                if error < best_tick_count:
                    best_tick_count = error
                    best_step = candidate_step
            
            y_step = best_step
        
        # Generate tick positions
        tick_start = math.floor(final_y_min / y_step) * y_step
        tick_end = math.ceil(final_y_max / y_step) * y_step
        y_ticks = np.arange(tick_start, tick_end + y_step/2, y_step)
        
        # Format tick labels
        if y_type == "int" or (y_type == "auto" and all(abs(t - round(t)) < 1e-9 for t in y_ticks)):
            y_labels = [str(int(t)) for t in y_ticks]
        else:
            y_labels = [f"{t:g}" for t in y_ticks]
        
        ax.set_yticks(y_ticks)
        ax.set_yticklabels(y_labels)
        
        # Set limits with guards (only if ylim was not explicitly provided)
        if ylim is None:
            y_span = final_y_max - final_y_min
            y_pad = y_guard * y_span if y_span > 0 else y_guard * y_step
            ax.set_ylim(final_y_min - y_pad, final_y_max + y_pad)


# ============================================================================
# Generic Plotting Functions
# ============================================================================

def plot_line(ax, x, y, yerr=None, label: Optional[str] = None,
              style: Optional[PlotStyle] = None,
              color_idx: int = 0, style_idx: int = 0, 
              show_markers: bool = False, **kwargs):
    """Generic line plot with optional error bars.
    
    Args:
        ax: Matplotlib axis
        x: X-axis data
        y: Y-axis data
        yerr: Y-axis error bars (optional)
        label: Legend label
        style: PlotStyle (uses ACM_COMPACT_HALF if None)
        color_idx: Index into style.colors
        style_idx: Index into style.line_styles and style.markers
        show_markers: Whether to show markers on line (default: False)
        **kwargs: Additional matplotlib arguments (overrides defaults)
        
    Returns:
        Modified axis
    """
    style = style or ACM_COMPACT_HALF
    color = kwargs.pop('color', style.colors[color_idx % len(style.colors)])
    linestyle = kwargs.pop('linestyle', style.line_styles[style_idx % len(style.line_styles)])
    
    # Only add markers if explicitly requested
    if show_markers:
        marker = kwargs.pop('marker', style.markers[style_idx % len(style.markers)])
        marker_size = style.marker_size
    else:
        marker = kwargs.pop('marker', None)
        marker_size = 0
    
    if yerr is not None:
        ax.errorbar(x, y, yerr=yerr, label=label, color=color,
                   linestyle=linestyle, marker=marker,
                   linewidth=style.line_width, markersize=marker_size,
                   capsize=3, elinewidth=1.2, **kwargs)
    else:
        ax.plot(x, y, label=label, color=color, linestyle=linestyle,
               marker=marker, linewidth=style.line_width,
               markersize=marker_size, **kwargs)
    return ax


def plot_stacked_area(ax, x, y_series: Dict[str, np.ndarray],
                     style: Optional[PlotStyle] = None,
                     color_map: Optional[Dict[str, str]] = None, **kwargs):
    """Generic stacked area plot.
    
    Args:
        ax: Matplotlib axis
        x: X-axis data
        y_series: Dictionary mapping label -> y-values (in stacking order)
        style: PlotStyle (uses ACM_COMPACT_HALF if None)
        color_map: Custom color mapping (uses style.colors if None)
        **kwargs: Additional matplotlib arguments
        
    Returns:
        Modified axis
        
    Example:
        >>> y_data = {"goodput": np.array([...]), "slo_violations": np.array([...])}
        >>> plot_stacked_area(ax, time_values, y_data)
    """
    style = style or ACM_COMPACT_HALF
    labels = list(y_series.keys())
    y_arrays = [y_series[label] for label in labels]
    
    if color_map:
        colors = [color_map.get(label, style.colors[i % len(style.colors)])
                 for i, label in enumerate(labels)]
    else:
        colors = [style.colors[i % len(style.colors)] for i in range(len(labels))]
    
    ax.stackplot(x, *y_arrays, labels=labels, colors=colors, alpha=0.75, **kwargs)
    return ax


def plot_grouped_bars(ax, x_positions, bar_groups: List[Tuple[str, List[float], Optional[List[float]]]],
                     style: Optional[PlotStyle] = None, **kwargs):
    """Generic grouped bar plot.
    
    Creates bars grouped at each x position, with different colored bars per group.
    
    Args:
        ax: Matplotlib axis
        x_positions: X-axis positions (e.g., [0, 1, 2, ...] for categorical data)
        bar_groups: List of (label, heights, errors) tuples
                   - label: Group legend label
                   - heights: Bar heights for each x position
                   - errors: Error bar values (None for no errors)
        style: PlotStyle (uses ACM_COMPACT_HALF if None)
        **kwargs: Additional matplotlib bar() arguments
        
    Returns:
        Modified axis
        
    Example:
        >>> bar_groups = [
        ...     ("Dagor", [10, 20, 30], [1, 2, 3]),
        ...     ("Rajomon", [15, 25, 35], [1.5, 2.5, 3.5]),
        ... ]
        >>> plot_grouped_bars(ax, [0, 1, 2], bar_groups)
    """
    style = style or ACM_COMPACT_HALF
    n_groups = len(bar_groups)
    bar_width = style.bar_width_fraction / n_groups
    
    for i, (label, heights, errors) in enumerate(bar_groups):
        offsets = [x - style.bar_width_fraction/2 + i*bar_width + bar_width/2
                  for x in x_positions]
        ax.bar(offsets, heights, bar_width * style.bar_spacing_fraction,
              yerr=errors, label=label,
              color=style.colors[i % len(style.colors)],
              edgecolor='black', linewidth=0.6,
              error_kw=dict(capsize=3, elinewidth=1.0), **kwargs)
    return ax


def plot_scatter(ax, x, y, yerr=None, label: Optional[str] = None,
                style: Optional[PlotStyle] = None,
                color_idx: int = 0, **kwargs):
    """Generic scatter plot with optional error bars.
    
    Args:
        ax: Matplotlib axis
        x: X-axis data
        y: Y-axis data
        yerr: Y-axis error bars (optional)
        label: Legend label
        style: PlotStyle (uses ACM_COMPACT_HALF if None)
        color_idx: Index into style.colors
        **kwargs: Additional matplotlib arguments
        
    Returns:
        Modified axis
    """
    style = style or ACM_COMPACT_HALF
    color = kwargs.pop('color', style.colors[color_idx % len(style.colors)])
    marker = kwargs.pop('marker', style.markers[color_idx % len(style.markers)])
    
    if yerr is not None:
        ax.errorbar(x, y, yerr=yerr, fmt='o', label=label, color=color,
                   marker=marker, markersize=style.marker_size,
                   linestyle='', capsize=3, **kwargs)
    else:
        ax.scatter(x, y, label=label, color=color, marker=marker,
                  s=style.marker_size**2, **kwargs)
    return ax

