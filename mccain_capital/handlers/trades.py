"""Trades endpoint handlers (delegating to service layer)."""

from mccain_capital.services import trades as svc
from mccain_capital.services import trades_forms
from mccain_capital.services import trades_mutations
from mccain_capital.services import trades_ops
from mccain_capital.services import trades_sync


def trades_page():
    return svc.trades_page()


def trades_duplicate(trade_id: int):
    return trades_mutations.trades_duplicate(trade_id)


def trades_delete(trade_id: int):
    return trades_mutations.trades_delete(trade_id)


def trades_delete_many():
    return trades_mutations.trades_delete_many()


def trades_copy_many():
    return trades_mutations.trades_copy_many()


def trades_set_setup(trade_id: int):
    return trades_mutations.trades_set_setup(trade_id)


def trades_set_setup_many():
    return trades_mutations.trades_set_setup_many()


def trades_edit(trade_id: int):
    return trades_forms.trades_edit(trade_id)


def trades_review(trade_id: int):
    return trades_forms.trades_review(trade_id)


def trades_replay(trade_id: int):
    return trades_forms.trades_replay(trade_id)


def trades_clear():
    return trades_mutations.trades_clear()


def trades_paste():
    return svc.trades_paste()


def trades_playbook():
    return svc.trades_playbook()


def trades_new_manual():
    return svc.trades_new_manual()


def trades_paste_broker():
    return svc.trades_paste_broker()


def trades_upload_pdf():
    return svc.trades_upload_pdf()


def trades_sync_live():
    return trades_sync.trades_sync_live()


def trades_sync_auto_config():
    return trades_sync.trades_sync_auto_config()


def trades_sync_auto_run_now():
    return trades_sync.trades_sync_auto_run_now()


def trades_sync_job_status(job_id: str):
    return trades_sync.trades_sync_job_status(job_id)


def trades_sync_job_cancel(job_id: str):
    return trades_sync.trades_sync_job_cancel(job_id)


def trades_sync_debug_file(name: str):
    return svc.trades_sync_debug_file(name)


def ops_alerts_page():
    return svc.ops_alerts_page()


def ops_alert_ack():
    return svc.ops_alert_ack()


def ops_alert_resolve():
    return svc.ops_alert_resolve()


def ops_alert_mute():
    return svc.ops_alert_mute()


def ops_backups_config():
    return svc.ops_backups_config()


def ops_backups_page():
    return trades_ops.ops_backups_page()


def ops_backups_run_now():
    return trades_ops.ops_backups_run_now()


def ops_backups_download(name: str):
    return trades_ops.ops_backups_download(name)


def ops_backups_restore():
    return trades_ops.ops_backups_restore()


def ops_backups_clear_live():
    return svc.ops_backups_clear_live()


def ops_backups_restore_dry_run():
    return trades_ops.ops_backups_restore_dry_run()


def ops_backups_delete():
    return trades_ops.ops_backups_delete()


def ops_job_status(job_id: str):
    return trades_ops.ops_job_status(job_id)


def ops_integrity_job_status(job_id: str):
    return trades_ops.ops_integrity_job_status(job_id)


def ops_integrity_run():
    return trades_ops.ops_integrity_run()


def rollback_import_batch():
    return svc.rollback_import_batch()


def trades_risk_controls():
    return trades_forms.trades_risk_controls()


def trades_open_positions():
    return svc.trades_open_positions()


def trades_rebuild_reviews():
    return svc.trades_rebuild_reviews()


def trades_update_balance_bases():
    return svc.trades_update_balance_bases()
