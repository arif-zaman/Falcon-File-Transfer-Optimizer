package client.utils;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by earslan on 11/3/16.
 */
public class Statistics
{
  List<Double> data = new LinkedList<>();
  List<Double> out_of_order_data = new LinkedList<>();
  int limit;

  public Statistics(int size) {
    this.limit = size;
  }
  public double getMean()
  {
    double sum = 0.0;
    for(double a : data)
      sum += a;
    return sum/limit;
  }

  public double getVariance()
  {
    double mean = getMean();
    double temp = 0;
    for(double a :data)
      temp += (a-mean)*(a-mean);
    return temp/limit;
  }

  public double getStdDev()
  {
    return Math.sqrt(getVariance());
  }

  public int getSize() {
    return data.size();
  }

  public int getOutOfOrderSize() {
    return out_of_order_data.size();
  }

  public int getLimit() {
    return limit;
  }

  public boolean addValue(double value) {
    data.add(value);
    if (data.size() > limit)
      data.remove(0);
    return true;
  }
  public boolean addOutOfOrderValue(double value) {
    return out_of_order_data.add(value);
  }

  public void clearOutOfOrderData() {
    out_of_order_data.clear();
  }

  public void makeOutOfOrderNewNormal() {
    data.clear();
    data.addAll(out_of_order_data);
    out_of_order_data.clear();
  }
}
