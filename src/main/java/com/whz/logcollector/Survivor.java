package com.whz.logcollector;

/**
 * 幸存区
 *
 * @author whz
 * @date 2022/1/11 22:26
 **/
public class Survivor {
    private boolean area;
    public Survivor(){
        this.area=true;
    }

    public boolean getArea() {
        return this.area;
    }

    public void changeArea() {
        this.area = !this.area;
    }
}
