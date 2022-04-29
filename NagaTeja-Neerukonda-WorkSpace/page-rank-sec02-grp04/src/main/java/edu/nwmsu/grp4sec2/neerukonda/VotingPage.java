package edu.nwmsu.grp4sec2.neerukonda;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class VotingPage extends DoFn<KV<String,Iterable<String>>,KV<String,RankedPage>> implements Serializable{

    public String voterName;
    public Integer contributorVotes;
    double pageRank = 1.0;

    public VotingPage(String voterName, Integer contributorVotes) {
        this.voterName = voterName;
        this.contributorVotes = contributorVotes;
      
    }

     public VotingPage(String voterName, Integer contributorVotes,double pageRank) {
        this.voterName = voterName;
        this.contributorVotes = contributorVotes;
        this.pageRank=pageRank;
    }



    public String getVoterName(){
        return voterName;
    }

    public  Integer getContributorVotes(){
        return contributorVotes;
    }

    public void setVoterName(String voterName){
        this.voterName = voterName;
    }

    public void setContributorVotes(Integer contributorVotes ){
        this.contributorVotes = contributorVotes;
    }

 
    public double getPageRank() {
        return this.pageRank;
    }

    public void setPageRank(double pageRank){
        this.pageRank = pageRank;
    }


    @Override
    public String toString() {
        return "voter Name = "+ voterName +", Page rank = "+this.pageRank +" contributorVotes = " + contributorVotes;
    }

}