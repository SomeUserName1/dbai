CREATE INDEX idx_sid ON Sailors(sid);
CREATE UNIQUE INDEX uidx_sid ON Sailors(sid);
CREATE INDEX idx_name USING LHASH ON Sailors(sname);
CREATE INDEX idx_rathing USING BTREE ON Sailors(rating DESC);
CREATE INDEX hash_idx_name USING EHASH ON Sailors(sname(10));
CREATE INDEX tree_idx_name USING BTREE ON Sailors(sname(10) ASC);