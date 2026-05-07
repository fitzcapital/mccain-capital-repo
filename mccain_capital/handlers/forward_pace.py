"""Forward Pace endpoint handlers."""

from mccain_capital.services import forward_pace as svc


def forward_pace_page():
    return svc.forward_pace_page()


def api_projection():
    return svc.api_projection()


def download_pdf():
    return svc.download_pdf()
