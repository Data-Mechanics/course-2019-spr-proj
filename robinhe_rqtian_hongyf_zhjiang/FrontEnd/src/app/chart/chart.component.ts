import { Component, OnInit } from '@angular/core';
import {Chart} from 'chart.js' ;
import {CrashdataService} from '../crashdata.service';
import {delay} from 'rxjs/operators';




@Component({
  selector: 'app-chart',
  templateUrl: './chart.component.html',
  styleUrls: ['./chart.component.css'],
  providers: [CrashdataService]
})
export class ChartComponent implements OnInit {
  url: string ;
  // Declaring the Promise, yes! Promise!
  filtersLoaded: Promise<boolean>;
  public chartLabel = ['2001', '2002', '2003', '2004', '2005', '2006', '2007',
    '2008', '2009' , '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018' ] ;
  public weather = ['Cloudy', 'Rain', 'Clear', 'Sleet', 'Fog', 'Unknown'] ;
  public severity = ['Fatal', 'Non-fatal', 'Property', 'Unknown'] ;
  public surface = ['Dry', 'Wet', 'Snow', 'Ice', 'Water', 'Sand', 'Slush', 'Unknown'] ;
  public ambient = ['DarkLighted', 'Dark', 'DarkUnknown', 'DayLight', 'Dusk', 'Dawn', 'Unknown'] ;
  baseline = localStorage.getItem('baseline') ;
  category: any = [];
  data ;

  public chartType = 'line' ;
  public chartLegend = true ;
  public chartData = [];
  public chartOption = {
    responsive : true
  } ;



  constructor(private dataService: CrashdataService) { }

  async ngOnInit() {
      this.getJson() ;
  }


  async getJson() {
    if (this.baseline === 'weather') { this.category = this.weather ; }
    if (this.baseline === 'severity') { this.category = this.severity ; }
    if (this.baseline === 'surface') { this.category = this.surface ; }
    if (this.baseline === 'ambient') { this.category = this.ambient ; }
    this.data = await this.dataService.getData().toPromise() ;
    for (let item of this.category) {
      const oneLine = {data: this.data[item], label: item, fill: false} ;
      await this.chartData.push(oneLine) ;
    }
    this.filtersLoaded = Promise.resolve(true) ;
    console.log(this.data) ;
  }


}
