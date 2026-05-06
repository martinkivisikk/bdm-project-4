# Output: write the flat text representation to screens_metadata.parsed_text
# (UPDATE ... WHERE screen_id = ...) for each screen processed in this run.
#
# Downstream tasks (embed_text, extract) read parsed_text directly from Postgres
# rather than via XCom — this keeps tasks independent and avoids XCom size limits
# when batches grow beyond a few screens.
