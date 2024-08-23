def _process_query(sys,queries,ld):
    tables = []
    for q in queries:
        try:
            if len(q) == 4:
                t = ld.query(table=q[0], path=q[1], query=q[2], json_depth=q[3])
            else:
                t = ld.query(table=q[0], path=q[1], query=q[2])
            tables += t
        except (ValueError, RuntimeError) as e:
            print('folio_demo.py: error processing "' + q[1] + '": ' + str(e), file=sys.stderr)
print()