import { Injectable } from '@angular/core';
import {HttpClient} from '@angular/common/http' ;
import { map } from 'rxjs/operators';

@Injectable({
  providedIn: 'root'
})
export class CrashdataService {
  url = localStorage.getItem('url') ;
  constructor(public http: HttpClient) { }

  getData() {

    // console.log('data from ' + url);
    // $http

    return this.http.get(this.url ).pipe(map((crashdata) => {
      return crashdata;
    }));
  }
}
