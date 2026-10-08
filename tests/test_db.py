from ct_pipeline.db import connect


def test_connect_spills_under_work_dir_not_cwd(tmp_path, monkeypatch):
    readonly = tmp_path / "readonly"
    readonly.mkdir()
    readonly.chmod(0o555)
    monkeypatch.chdir(readonly)
    work = tmp_path / "work"

    con = connect(work)
    con.execute("SET memory_limit = '100MB'")
    con.execute("SET threads = 1")
    con.execute("SET preserve_insertion_order = false")
    temp_dir = con.execute("SELECT current_setting('temp_directory')").fetchone()[0]
    con.execute(
        "CREATE TABLE t AS SELECT i, md5(i::VARCHAR) || md5((i + 1)::VARCHAR) AS s FROM range(2000000) r(i) ORDER BY s"
    )
    con.close()

    assert temp_dir.startswith(str(work))
    assert list(readonly.iterdir()) == []
