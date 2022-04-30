package edu.nwmsu.grp4sec2.Dipika;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;

import org.apache.beam.sdk.values.KV;

public class RankedPage implements Serializable , Comparator<KV<Double,String>>{
    public String key;
    public double rank = 1.0;
    public ArrayList<VotingPage> voters;

     public RankedPage(String key,double rank, ArrayList<VotingPage> voters){
        this.key = key;
        this.voters = voters;
        this.rank = rank;
    }    


    public RankedPage(String key, ArrayList<VotingPage> voters) {
        this.key = key;
        this.voters = voters;
    }
    public RankedPage() {
        key = "";
        rank = 0.0;
    }
    public String getKey(){
        return key;
    }

    public  ArrayList<VotingPage> getVoters(){
        return voters;
    }

    public void setKey(String key){
        this.key = key;
    }

    public  void setVoters(ArrayList<VotingPage> voters){
        this.voters = voters;
    }
     public double getRank() {
        return this.rank;
    }
    @Override
    public String toString(){
        return this.key +"<"+ this.rank +","+ voters +">";
    }
     @Override
    public int compare(KV<Double, String> o1, KV<Double, String> o2) {
        double rank1 = o1.getKey();
        double rank2 = o2.getKey();
        if (rank1 > rank2) {
            return 1;
        } else if(rank1 < rank2) {
            return -1;
        }else{
            return 0;
        }
    }


}

