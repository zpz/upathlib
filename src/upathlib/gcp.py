# Back compatibility bridge. Will remove in 0.6.9.
from . import gcs

GcpBlobUpath = gcs.GcsBlobUpath

#  Users may want to
# add very thin wrappers in their application code to handle credentials for their cloud accounts.
