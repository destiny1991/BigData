package edu.buaa.MapReduce;

public class Math {
	private int M;
	
	public void combine(int[] a, int n, int[] b, int m) {
		for(int i=n; i>=m; i--) {
			b[m-1] = i - 1;
			if(m > 1) {
				combine(a, n-1, b, m-1);
			} else {
				for(int j=M-1; j>=0; j--) {
					System.out.print(a[b[j]] + " ");
				}
				System.out.println();
			}
		}
	}
	
	public static void main(String[] args) {
		Math s = new Math();
		s.M = 3;
		int n = 5;
		int m = 3;
		int[] a = new int[n];
		int[] b = new int[m];
		
		for(int i=0; i<n; i++) a[i] = n - i - 1;
		
		s.combine(a, n, b, m);
	}
}
