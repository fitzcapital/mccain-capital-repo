"""UI rendering adapters for service modules.

Keeps render concerns centralized while legacy templates remain in app_core.
"""

from mccain_capital import app_core as core

render_page = core.render_page
simple_msg = core._simple_msg
