import {MediaMatcher} from '@angular/cdk/layout';

import { ChangeDetectorRef, Component } from '@angular/core';
import {MatListModule} from '@angular/material/list';
import {MatSidenavModule} from '@angular/material/sidenav';
import {MatIconModule} from '@angular/material/icon';
import {MatButtonModule} from '@angular/material/button';
import {MatToolbarModule} from '@angular/material/toolbar';
import { BidiModule, Directionality } from '@angular/cdk/bidi';
import { ActivatedRoute, RouterModule } from '@angular/router';
import { DomSanitizer, SafeResourceUrl, SafeUrl } from '@angular/platform-browser';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [
    MatToolbarModule, MatButtonModule, MatIconModule, MatSidenavModule, MatListModule, BidiModule, RouterModule,
  ],
  templateUrl: './app.component.html',
  styleUrl: './app.component.less'
})
export class AppComponent {
  mobileQuery: MediaQueryList;

  private _mobileQueryListener: () => void;

  botList = [
    {
      slug: 'budget',
      title: 'נתונים תקציביים',
      // url: 'https://udify.app/chatbot/ICgina67amgdZ8lS'
      url: 'https://udify.app/chat/ICgina67amgdZ8lS'
    },
    {
      slug: 'knesset-bylaws',
      title: 'תקנון הכנסת',
      url: 'https://udify.app/chat/V7kD8BO12FERMLT9'
    }
  ]
  open_: boolean;
  selectedUrl: SafeResourceUrl | null = null;
  selected: any = null;

  constructor(private changeDetectorRef: ChangeDetectorRef, media: MediaMatcher, dir: Directionality, route: ActivatedRoute, sanitizer: DomSanitizer) {
    this.mobileQuery = media.matchMedia('(max-width: 600px)');
    this.mobileQuery.addEventListener('change', () => this.mobileQueryListener());
    this.open_ = !this.mobileQuery.matches;
    route.fragment.subscribe(f => {
      this.selected = null;
      for (let bot of this.botList) {
        if (bot.slug === f) {
          this.selected = bot;
          break;
        }
      }
      if (this.selected) {
        if (this.mobileQuery.matches) {
          this.open = false;
        }
        this.selectedUrl = this.selected ? sanitizer.bypassSecurityTrustResourceUrl(this.selected.url) : null;
      }
    });
  }

  ngOnDestroy(): void {
    this.mobileQuery.removeEventListener('change', this._mobileQueryListener);
  }

  mobileQueryListener() {
    this.open = !this.mobileQuery.matches;
    console.log('mobileQueryListener', this.open);
    this.changeDetectorRef.detectChanges()
  }
  
  set open(value: boolean) {
    this.open_ = value;
  }

  get open(): boolean {
    return this.open_;
  }
}
