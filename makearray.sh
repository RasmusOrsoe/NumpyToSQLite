~/.local/bin/i3cols extr_sep  ~/i3_workspace/data/oscNext/genie/120000/oscNext_genie_level5_v01.01_pass2.120000.*\
    --outdir  ~/i3_workspace/arrays \
    --tempdir ~/i3_workspace/arrays/tmps \
    --procs 20 \
    --concatenate-and-index-by subrun \
    --keys I3EventHeader \
           I3MCTree \
           MCInIcePrimary \
           SplitInIcePulses
           
