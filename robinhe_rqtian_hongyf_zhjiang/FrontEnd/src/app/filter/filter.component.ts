import { Component, OnInit } from '@angular/core';
import {Router} from '@angular/router';
import {CrashdataService} from '../crashdata.service';

import { map } from 'rxjs/operators';

@Component({
  selector: 'app-filter',
  templateUrl: './filter.component.html',
  styleUrls: ['./filter.component.css'],
  providers: [CrashdataService]
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
    if(this.selectedLink !== '') {
      const lastParent = document.getElementById(this.selectedLink).children ;
      for (let i = 0; i < lastParent.length; i++) {
        lastParent[i].removeAttribute('disabled') ;
      }
    }
    this.selectedLink = e;
    console.log(this.selectedLink) ;
    const parent = document.getElementById(this.selectedLink).children ;
    for (let i = 0; i < parent.length; i++) {
        parent[i].setAttribute('disabled', 'disabled') ;
    }


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
  Surface1() {

    const mybox = document.getElementById('surface1') as HTMLInputElement ;
    if (mybox.checked) {
      this.surface.push(mybox.value) ;
    } else {
      const index: number = this.surface.indexOf(mybox.value);
      if (index !== -1) {
        this.surface.splice(index, 1);
      }
    }
    console.log(this.surface) ;
  }
  Surface2() {

    const mybox = document.getElementById('surface2') as HTMLInputElement ;
    if (mybox.checked) {
      this.surface.push(mybox.value) ;
    } else {
      const index: number = this.surface.indexOf(mybox.value);
      if (index !== -1) {
        this.surface.splice(index, 1);
      }
    }
    console.log(this.surface) ;
  }Surface3() {

    const mybox = document.getElementById('surface3') as HTMLInputElement ;
    if (mybox.checked) {
      this.surface.push(mybox.value) ;
    } else {
      const index: number = this.surface.indexOf(mybox.value);
      if (index !== -1) {
        this.surface.splice(index, 1);
      }
    }
    console.log(this.surface) ;
  }Surface4() {

    const mybox = document.getElementById('surface4') as HTMLInputElement ;
    if (mybox.checked) {
      this.surface.push(mybox.value) ;
    } else {
      const index: number = this.surface.indexOf(mybox.value);
      if (index !== -1) {
        this.surface.splice(index, 1);
      }
    }
    console.log(this.surface) ;
  }Surface5() {

    const mybox = document.getElementById('surface5') as HTMLInputElement ;
    if (mybox.checked) {
      this.surface.push(mybox.value) ;
    } else {
      const index: number = this.surface.indexOf(mybox.value);
      if (index !== -1) {
        this.surface.splice(index, 1);
      }
    }
    console.log(this.surface) ;
  }Surface6() {

    const mybox = document.getElementById('surface6') as HTMLInputElement ;
    if (mybox.checked) {
      this.surface.push(mybox.value) ;
    } else {
      const index: number = this.surface.indexOf(mybox.value);
      if (index !== -1) {
        this.surface.splice(index, 1);
      }
    }
    console.log(this.surface) ;
  }Surface7() {

    const mybox = document.getElementById('surface7') as HTMLInputElement ;
    if (mybox.checked) {
      this.surface.push(mybox.value) ;
    } else {
      const index: number = this.surface.indexOf(mybox.value);
      if (index !== -1) {
        this.surface.splice(index, 1);
      }
    }
    console.log(this.surface) ;
  }Surface8() {

    const mybox = document.getElementById('surface8') as HTMLInputElement ;
    if (mybox.checked) {
      this.surface.push(mybox.value) ;
    } else {
      const index: number = this.surface.indexOf(mybox.value);
      if (index !== -1) {
        this.surface.splice(index, 1);
      }
    }
    console.log(this.surface) ;
  }
  Ambient1() {

    const mybox = document.getElementById('ambient1') as HTMLInputElement ;
    if (mybox.checked) {
      this.ambient.push(mybox.value) ;
    } else {
      const index: number = this.ambient.indexOf(mybox.value);
      if (index !== -1) {
        this.ambient.splice(index, 1);
      }
    }
    console.log(this.ambient) ;
  }
  Ambient2() {

    const mybox = document.getElementById('ambient2') as HTMLInputElement ;
    if (mybox.checked) {
      this.ambient.push(mybox.value) ;
    } else {
      const index: number = this.ambient.indexOf(mybox.value);
      if (index !== -1) {
        this.ambient.splice(index, 1);
      }
    }
    console.log(this.ambient) ;
  }
  Ambient3() {

    const mybox = document.getElementById('ambient3') as HTMLInputElement ;
    if (mybox.checked) {
      this.ambient.push(mybox.value) ;
    } else {
      const index: number = this.ambient.indexOf(mybox.value);
      if (index !== -1) {
        this.ambient.splice(index, 1);
      }
    }
    console.log(this.ambient) ;
  }
  Ambient4() {

    const mybox = document.getElementById('ambient4') as HTMLInputElement ;
    if (mybox.checked) {
      this.ambient.push(mybox.value) ;
    } else {
      const index: number = this.ambient.indexOf(mybox.value);
      if (index !== -1) {
        this.ambient.splice(index, 1);
      }
    }
    console.log(this.ambient) ;
  }
  Ambient5() {

    const mybox = document.getElementById('ambient5') as HTMLInputElement ;
    if (mybox.checked) {
      this.ambient.push(mybox.value) ;
    } else {
      const index: number = this.ambient.indexOf(mybox.value);
      if (index !== -1) {
        this.ambient.splice(index, 1);
      }
    }
    console.log(this.ambient) ;
  }
  Ambient6() {

    const mybox = document.getElementById('ambient6') as HTMLInputElement ;
    if (mybox.checked) {
      this.ambient.push(mybox.value) ;
    } else {
      const index: number = this.ambient.indexOf(mybox.value);
      if (index !== -1) {
        this.ambient.splice(index, 1);
      }
    }
    console.log(this.ambient) ;
  }
  Ambient7() {

    const mybox = document.getElementById('ambient7') as HTMLInputElement ;
    if (mybox.checked) {
      this.ambient.push(mybox.value) ;
    } else {
      const index: number = this.ambient.indexOf(mybox.value);
      if (index !== -1) {
        this.ambient.splice(index, 1);
      }
    }
    console.log(this.ambient) ;
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
    if (this.selectedLink === '') {
      alert('You have to choose one baseline') ;
    } else {
      this.route.navigate(['chart']);
    }

    localStorage.setItem('baseline', this.selectedLink) ;
  }
}
