import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { FilterComponent } from './filter/filter.component';
import { DataComponent } from './data/data.component';
import { ChartComponent } from './chart/chart.component';

const routes: Routes = [
  {
    path: '',
    redirectTo: 'filter',
    pathMatch: 'full'
  },
  {
    path: 'filter',
    component: FilterComponent
  },
  {
    path: 'data',
    component: DataComponent
  },
  {
    path: 'chart',
    component: ChartComponent
  }
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
