import { TestBed } from '@angular/core/testing';

import { CrashdataService } from './crashdata.service';

describe('CrashdataService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: CrashdataService = TestBed.get(CrashdataService);
    expect(service).toBeTruthy();
  });
});
