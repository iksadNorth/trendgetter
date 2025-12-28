"""initial tfidf_scores table

Revision ID: 001_initial_tfidf_scores
Revises: 
Create Date: 2025-01-01 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '001_initial_tfidf_scores'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'tfidf_scores',
        sa.Column('id', sa.Integer(), nullable=False, autoincrement=True),
        sa.Column('token', sa.String(length=255), nullable=False),
        sa.Column('score', sa.Float(), nullable=False),
        sa.Column('tf', sa.Float(), nullable=False),
        sa.Column('idf', sa.Float(), nullable=False),
        sa.Column('start_time', sa.DateTime(), nullable=False),
        sa.Column('end_time', sa.DateTime(), nullable=False),
        sa.Column('bucket_count', sa.Integer(), nullable=False),
        sa.Column('total_bucket_count', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(
        'idx_tfidf_scores_time_range',
        'tfidf_scores',
        ['start_time', 'end_time'],
        unique=False
    )
    op.create_index(
        'idx_tfidf_scores_token',
        'tfidf_scores',
        ['token'],
        unique=False
    )


def downgrade() -> None:
    op.drop_index('idx_tfidf_scores_token', table_name='tfidf_scores')
    op.drop_index('idx_tfidf_scores_time_range', table_name='tfidf_scores')
    op.drop_table('tfidf_scores')

