"""add anomaly fields to jobs

Revision ID: f3a91c7d5b02
Revises: b1c66183842b
Create Date: 2026-08-05 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'f3a91c7d5b02'
down_revision: Union[str, Sequence[str], None] = 'b1c66183842b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('jobs', sa.Column('duration_seconds', sa.Float(), nullable=True))
    op.add_column('jobs', sa.Column('anomaly_score', sa.Float(), nullable=True))
    op.add_column('jobs', sa.Column('is_anomaly', sa.Boolean(), nullable=False, server_default='false'))
    op.create_index(op.f('ix_jobs_is_anomaly'), 'jobs', ['is_anomaly'])


def downgrade() -> None:
    op.drop_index(op.f('ix_jobs_is_anomaly'), table_name='jobs')
    op.drop_column('jobs', 'is_anomaly')
    op.drop_column('jobs', 'anomaly_score')
    op.drop_column('jobs', 'duration_seconds')