package com.zll.gmall.realtime;

import javax.swing.tree.TreeNode;
import java.util.LinkedList;

/**
 * @ClassName TreeNodeTest
 * @Description TODO
 * @Author 17588
 * @Date 2021-05-25 11:29
 * @Version 1.0
 */
public class TreeNodeTest {
    public static void main(String[] args) {
        int array[] = new int[]{6, 1, 6, 3, 1, 2, 7, 8};
        sort1(array);
        for (int i = array.length - 1; i >= 0; i--) {
            System.out.print(array[i]);
        }
    }
//    public static  void sort(int array[]) {
//        for (int i = 0; i < array.length - 1; i++) {
//            for (int j = 0; j < array.length - 1 - i; j++) {
//                int a = 0;
//                if (array[j] > array[j + 1]){
//                    a = array[j];
//                    array[j] = array[j + 1];
//                    array[j + 1] = a;
//                }
//            }
//
//        }
//    }
//
//    public static void sort1(int array[]) {
//        for (int i = 0; i < array.length - 1; i++) {
//            boolean isSorted = true;
//            for (int j = 0; j < array.length - 1 - i; j++) {
//                int a = 0;
//                if (array[j] > array[j + 1]) {
//                    a = array[j];
//                    array[j] = array[j + 1];
//                    array[j + 1] = a;
//                    isSorted = false;
//                }
//            }
//            if(isSorted) {
//                break;
//            }
//        }
//    }

    public static void sort1(int[] arr) {
        for (int i = 0; i < arr.length - 1; i++) {
                boolean isFlag = true;
            for (int j = 0; j < arr.length - 1 - i; j++) {
                int tmp = 0;
                if (arr[j] < arr[j + 1]) {
                    tmp = arr[j];
                    arr[j] = arr[j + 1];
                    arr[j + 1] = tmp;
                    isFlag = false;
                }
            }
            if (isFlag) {
                break;
            }
        }
    }


    public static void sort2(int[] arr) {

    }




//    public static TreeNode createBinaryTree(LinkedList<Integer> inputlist) {
//        TreeNode node = null;
//        if (inputlist == null || inputlist.isEmpty()) {
//            return null;
//        }
//        Integer data = inputlist.removeFirst();
//        if (data != null) {
//            node = new TreeNode(data);
//
//        }
//
//        return node;
//    }

}

