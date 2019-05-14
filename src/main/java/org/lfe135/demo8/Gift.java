package org.lfe135.demo8;

public class Gift {
	private Long gfid;
	private Long gfcnt;
	private Long hits;
	public Gift() {
		gfid=0L;
		gfcnt=1L;
		hits=1L;
	}
	public Long getGfid() {
		return gfid;
	}
	public void setGfid(Long gfid) {
		this.gfid = gfid;
	}
	public Long getGfcnt() {
		return gfcnt;
	}
	public void setGfcnt(Long gfcnt) {
		this.gfcnt = gfcnt;
	}
	public Long getHits() {
		return hits;
	}
	public void setHits(Long hits) {
		this.hits = hits;
	}
}