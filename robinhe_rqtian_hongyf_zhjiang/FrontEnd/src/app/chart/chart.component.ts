import { Component, OnInit } from '@angular/core';
import {Chart} from 'chart.js' ;
import {CrashdataService} from '../crashdata.service';
import {HttpClient} from '@angular/common/http' ;
import { map } from 'rxjs/operators';

@Component({
  selector: 'app-chart',
  templateUrl: './chart.component.html',
  styleUrls: ['./chart.component.css'],
  providers: [CrashdataService]
})
export class ChartComponent implements OnInit {
  url: string ;
  public chartLabel = ['2001', '2002', '2003', '2004', '2005', '2006', '2007',
    '2008', '2009' , '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018' ] ;
  public weather: any = ['Cloudy', 'Rain', 'Clear', 'Sleet', 'Fog', 'Unknown'] ;
  public severity: any = ['Fatal', 'Non-fatal', 'Property', 'Unknown'] ;
  public surface: any = ['Dry', 'Wet', 'Snow', 'Ice', 'Water', 'Sand', 'Slush', 'Unknown'] ;
  public ambient: any = ['DarkLighted', 'Dark', 'DarkUnknown', 'DayLight', 'Dusk', 'Dawn', 'Unknown'] ;
  baseline = localStorage.getItem('baseline') ;
  data ;

  public chartType = 'line' ;
  public chartLegend = true ;
  public chartData = [
  ];
  public chartOption = {
    responsive : true
  } ;



  constructor(private dataService: CrashdataService) { }

  ngOnInit() {
    this.url = localStorage.getItem('url') ;
    console.log('this is the url' + this.url) ;
    this.dataService.getData().subscribe(crashdata => {
      this.data = crashdata ;
      console.log(this.data) ;
      for ( let i = 0 ; i < this.baseline.length ; i++ ) {
        let line = {data: this.data[this.baseline[i]] , label: this.baseline[i]} ;
        this.chartData.push(line) ;
      }

    });

  }
}
