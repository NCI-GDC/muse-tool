from cdis_pipe_utils import postgres


class MuSE(postgres.ToolTypeMixin, postgres.Base):

    __tablename__ = 'muse_metrics'
