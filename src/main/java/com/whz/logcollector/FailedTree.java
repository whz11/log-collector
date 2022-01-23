package com.whz.logcollector;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

/**
 * 使用败者树，对已经排序的子文件进行多路归并排序
 *
 * @author whz
 * @date 2022/1/9 00:16
 **/
public class FailedTree<A, B extends BufferedReader> {

    /**
     * 败者树（有K个节点 --> k为子文件的数量），保存失败下标。tree[0]保存最小值的下标(胜利者)
     */
    private Integer[] tree;

    /**
     * 叶子节点数组的个数（即 K路归并中的K）
     */
    private int size;

    /**
     * 叶子节点（必须是可以比较的对象）
     */
    private final List<A> leaves;

    private final List<B> bufferList;

    private final BiPredicate<A, A> comparator;

    /**
     * 初始化最小值
     */
    private static final Integer MIN_KEY = -1;

    /**
     * 失败者树构造函数
     */
    public FailedTree(List<B> bufferList, BiPredicate<A, A> comparator) {
        this.size = bufferList.size();
        this.tree = new Integer[size];
        this.comparator = comparator;
        this.bufferList = bufferList;
        this.leaves = bufferList.stream()
                .map(b -> {
                    try {
                        return (A) b.readLine();
                    } catch (IOException e) {
                        e.printStackTrace();
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        //初始化败者树(其实就是一个普通的二叉树，共有2k - 1个节点)
        for (int i = 0; i < size; i++) {
            //初始化时，树种各个节点值设为最小值
            tree[i] = MIN_KEY;
        }
        //初始化，从最后一个（最小值最大的那个）节点开始调整，k次（小文件个数）调整
        for (int i = size - 1; i >= 0; i--) {
            adjust(i);
        }
    }

    /**
     * 从底向上调整数结构
     *
     * @param s 叶子节点数组的下标(第几个文件)
     */
    private void adjust(int s) {
        // tree[t] 是 leaves[s] 的父节点
        int t = (s + size) / 2;
        while (t > 0) {
            //如果叶子节点值大于父节点（保存的下标）指向的值
            if (s >= 0 && (tree[t] == -1 || comparator.test(leaves.get(s), leaves.get(tree[t])))) {
                //父节点保存其下标：总是保存较大的（败者）。 	较小值的下标（用s记录）->向上传递
                int temp = s;
                s = tree[t];
                tree[t] = temp;
            }
            // tree[Integer/2] 是 tree[Integer] 的父节点
            t /= 2;
        }
        //最后的胜者（最小值）
        tree[0] = s;
    }

    /**
     * 给叶子节点赋值
     *
     * @param leaf 叶子节点值
     * @param s    叶子节点的下标
     */
    public void add(A leaf, int s) {
        leaves.set(s, leaf);
        //每次赋值之后，都要向上调整，使根节点保存最小值的下标（找到当前最小值）
        adjust(s);
    }

    /**
     * 删除叶子节点，即一个归并段元素取空
     *
     * @param s 叶子节点的下标
     */
    public void del(int s) throws IOException {
        //当一个文件读取完成之后，关闭文件输入流、移除出List集合。
        this.bufferList.get(s).close();
        this.bufferList.remove(s);
        //删除叶子节点
        leaves.remove(s);
        this.size--;
        this.tree = new Integer[size];

        //初始化败者树（严格的说，此时它只是一个普通的二叉树）
        for (int i = 0; i < size; i++) {
            //初始化时，树中各个节点值设为可能的最小值
            tree[i] = MIN_KEY;
        }
        //从最后一个节点开始调整
        for (int i = size - 1; i >= 0; i--) {
            adjust(i);
        }

    }

    /**
     * 获得胜者(值为最终胜出的叶子节点的下标)
     **/
    public A getWinner() throws IOException {
        if (tree.length <= 0) {
            return null;
        }
        int index = tree[0];
        A winner = leaves.get(index);
        String newLeaf = this.bufferList.get(index).readLine();
        //如果文件读取完成，关闭读取流，删除叶子几点
        if (newLeaf == null) {
            del(index);
        } else {
            add((A) newLeaf, index);
        }
        return winner;

    }

}