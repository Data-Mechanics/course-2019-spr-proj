import { Component, OnInit } from '@angular/core';
import {Router} from '@angular/router';

import { map } from 'rxjs/operators';

@Component({
  selector: 'app-filter',
  templateUrl: './filter.component.html',
  styleUrls: ['./filter.component.css']
})
export class FilterComponent implements OnInit {

  public weather: any = ['Cloudy', 'Rain', 'Clear', 'Sleet', 'Fog', 'Unknown'] ;
  public severity: any = ['Fatal', 'Non-fatal', 'Property', 'Unknown'] ;
  public surface: any = ['Dry', 'Wet', 'Snow', 'Ice', 'Water', 'Sand', 'Slush', 'Unknown'] ;
  public ambient: any = ['DarkLighted', 'Dark', 'DarkUnknown', 'DayLight', 'Dusk', 'Dawn', 'Unknown'] ;
  public selectedLink: any = '';
  public baseline: any ;
  constructor(private route: Router) { }

  ngOnInit() {
  }


  setradio(e: string): void {

    this.selectedLink = e;
    console.log(this.selectedLink) ;


  }

  Weather1() {

    const mybox = document.getElementById('weather1') as HTMLInputElement ;
    if (mybox.checked) {
        this.weather.push(mybox.value) ;
    } else {
      const index: number = this.weather.indexOf(mybox.value);
      if (index !== -1) {
        this.weather.splice(index, 1);
      }
    }
    console.log(this.weather) ;
  }

  Weather2() {

    const mybox = document.getElementById('weather2') as HTMLInputElement ;
    if (mybox.checked) {
      this.weather.push(mybox.value) ;
    } else {
      const index: number = this.weather.indexOf(mybox.value);
      if (index !== -1) {
        this.weather.splice(index, 1);
      }
    }
    console.log(this.weather) ;
  }
  Weather3() {

    const mybox = document.getElementById('weather3') as HTMLInputElement ;
    if (mybox.checked) {
      this.weather.push(mybox.value) ;
    } else {
      const index: number = this.weather.indexOf(mybox.value);
      if (index !== -1) {
        this.weather.splice(index, 1);
      }
    }
    console.log(this.weather) ;
  }
  Weather4() {

    const mybox = document.getElementById('weather4') as HTMLInputElement ;
    if (mybox.checked) {
      this.weather.push(mybox.value) ;
    } else {
      const index: number = this.weather.indexOf(mybox.value);
      if (index !== -1) {
        this.weather.splice(index, 1);
      }
    }
    console.log(this.weather) ;
  }
  Weather5() {

    const mybox = document.getElementById('weather5') as HTMLInputElement ;
    if (mybox.checked) {
      this.weather.push(mybox.value) ;
    } else {
      const index: number = this.weather.indexOf(mybox.value);
      if (index !== -1) {
        this.weather.splice(index, 1);
      }
    }
    console.log(this.weather) ;
  }
  Weather6() {

    const mybox = document.getElementById('weather6') as HTMLInputElement ;
    if (mybox.checked) {
      this.weather.push(mybox.value) ;
    } else {
      const index: number = this.weather.indexOf(mybox.value);
      if (index !== -1) {
        this.weather.splice(index, 1);
      }
    }
    console.log(this.weather) ;
  }

  Severity1() {

    const mybox = document.getElementById('severity1') as HTMLInputElement ;
    if (mybox.checked) {
      this.severity.push(mybox.value) ;
    } else {
      const index: number = this.severity.indexOf(mybox.value);
      if (index !== -1) {
        this.severity.splice(index, 1);
      }
    }
    console.log(this.severity) ;
  }

  Severity2() {

    const mybox = document.getElementById('severity2') as HTMLInputElement ;
    if (mybox.checked) {
      this.severity.push(mybox.value) ;
    } else {
      const index: number = this.severity.indexOf(mybox.value);
      if (index !== -1) {
        this.severity.splice(index, 1);
      }
    }
    console.log(this.severity) ;
  }
  Severity3() {

    const mybox = document.getElementById('severity3') as HTMLInputElement ;
    if (mybox.checked) {
      this.severity.push(mybox.value) ;
    } else {
      const index: number = this.severity.indexOf(mybox.value);
      if (index !== -1) {
        this.severity.splice(index, 1);
      }
    }
    console.log(this.severity) ;
  }
  Severity4() {

    const mybox = document.getElementById('severity4') as HTMLInputElement ;
    if (mybox.checked) {
      this.severity.push(mybox.value) ;
    } else {
      const index: number = this.severity.indexOf(mybox.value);
      if (index !== -1) {
        this.severity.splice(index, 1);
      }
    }
    console.log(this.severity) ;
  }
  chartDataRequest() {
    if (this.selectedLink === 'weather') { this.baseline = this.weather ; }
    if (this.selectedLink === 'severity') { this.baseline = this.severity ; }
    if (this.selectedLink === 'surface') { this.baseline = this.surface ; }
    if (this.selectedLink === 'ambient') { this.baseline = this.ambient ; }
    let baselines: any = [this.weather, this.severity, this.surface, this.ambient] ;
    let baselineNames: any = ['weather', 'severity', 'surface', 'ambient'] ;
    const index: number = baselines.indexOf(this.baseline);
    console.log(index) ;
    if (index !== -1) {
      console.log(baselines) ;
      baselines.splice(index, 1);
      console.log(baselines) ;
    }

    const nameIndex: number = baselineNames.indexOf(this.selectedLink);
    if (index !== -1) {
      baselineNames.splice(nameIndex, 1);
      console.log(baselineNames) ;
    }

    let url = '/revere/api/v1/statistics?' ;
    url += 'baseline=' + this.selectedLink + '&';
    for (let j = 0 ; j < baselines.length ; j++ ) {
      if (baselines.length !== 0) {
        url += baselineNames[j] ;
        url += '=' ;
        for (let i = 0 ; i < baselines[j].length ; i++ ) {
          url += baselines[j][i] ;
          if (i === baselines[j].length - 1 ) {
            url += '&' ;
          } else {
            url += ',' ;
          }
        }
      }

   }
    url = url.substring(0, url.length - 1 ) ;
    console.log(url) ;
    localStorage.setItem('url', url) ;
    this.route.navigate(['chart']);
    localStorage.setItem('baseline', this.baseline) ;
  }
}
