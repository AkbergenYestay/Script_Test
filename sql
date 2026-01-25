DECLARE
    v_owner       VARCHAR2(30) := 'DSSB_DE';
    v_table       VARCHAR2(30) := 'RB2_FS_TRANS_IN';
    v_tablespace  VARCHAR2(30) := 'DSSB_NEW';  -- поменяй если другой

    d_start       DATE := DATE '2026-01-01';
    d_end         DATE := DATE '2026-02-01';   -- конец НЕ включается
    d             DATE;

    v_part_name   VARCHAR2(30);
    v_cnt         NUMBER;
BEGIN
    d := d_start;

    WHILE d < d_end LOOP
        v_part_name := 'P' || TO_CHAR(d, 'YYYYMMDD');

        SELECT COUNT(*)
          INTO v_cnt
          FROM all_tab_partitions
         WHERE table_owner = v_owner
           AND table_name  = v_table
           AND partition_name = v_part_name;

        IF v_cnt = 0 THEN
            EXECUTE IMMEDIATE
                'ALTER TABLE ' || v_owner || '.' || v_table ||
                ' ADD PARTITION ' || v_part_name ||
                ' VALUES LESS THAN (DATE ''' || TO_CHAR(d+1,'YYYY-MM-DD') || ''') ' ||
                ' TABLESPACE ' || v_tablespace;
        END IF;

        d := d + 1;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('Done: partitions created for 2026-01-01..2026-01-31');
END;
/
