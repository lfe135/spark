package org.lfe135.demo8;

import java.util.ArrayList;
import java.util.List;

/**
 * Hello world!
 *
 */
public class App16 {
	public static void main(String[] args) {
		System.out.println(lastStoneWeight(new int[] { 1, 2, 6, 4 }));
	}

	public String removeDuplicates(String S) {
		List<Character> a = new ArrayList<>();
		for (char b : S.toCharArray()) {
			a.add(b);
		}
		boolean c = true;
		while (c) {
			c = false;
			for (int i = 1; i < a.size(); i++) {
				if (a.get(i).equals(a.get(i - 1))) {
					a.remove(i);
					a.remove(i - 1);
					i -= 2;
					c = true;
				}
			}
		}
		String s = "";
		for (char d : a) {
			s += d;
		}
		return s;
	}

	public static int lastStoneWeight(int[] stones) {
		// 三个中的最小的\
		int e = 0;
		for (int d = 0; d < stones.length; d++) {
			int a = 0, b = 1;
			for (int i = 2; i < stones.length; i++) {
				if (stones[i] > stones[a]) {
					if (stones[a] > stones[b]) {
						b = i;
					} else {
						a = i;
					}
				} else if (stones[i] > stones[b]) {
					b = i;
				}
			}
			int c = stones[a] - stones[b];
			if (c < 0) {
				c = c * -1;
				stones[b] = c;
				stones[a] = 0;
			} else {
				stones[a] = c;
				stones[b] = 0;
			}
			e = c;
		}
		return e;
	}
}