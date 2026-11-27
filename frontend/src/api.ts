import type {FeatureCollection,Geometry} from 'geojson';
export type Filters={resolution:number;start:number;end:number};
export type Meta={years:number[];resolutions:number[];cells:number;manifest:unknown;environment_years?:number[]};
export type Cell={cell_id:string;area_ha:number;territory_ha:number;intensity:number;distance_km:number;road_density:number;cluster?:string;forest_pct?:number;forest_ha?:number;forest_ratio?:number|null;ring_id?:string};
export type Grid=FeatureCollection<Geometry,Cell>;
export type Annual={year:number;area_ha:number;intensity:number};
export type Band={band:string;area_ha:number;territory_ha:number;cells:number;intensity:number|null};
export type Analysis={summary:{area_ha:number;territory_ha:number;intensity:number;cells:number;affected:number};annual:Annual[];bands:Band[];heat:{year:number;band:string;intensity:number}[];association:{rho:number|null;n:number}};
export type Moran={global:{I:number;expected:number;p:number;n:number;islands:number;permutations:number;seed:number}|null;local:{cell_id:string;cluster:string;I:number;p:number;q:number;z:number;lag:number}[];reason?:string};
export type Environment={annual:{year:number;area_ha:number;forest_ha:number;ratio_pct:number}[];bands:{band:string;ratio_pct:number;forest_ha:number;area_ha:number}[];n:number;excluded:number;excluded_area_ha:number;provider:string;note:string};
export type Model={exposure?:string;coefficients:{term:string;beta:number;ratio:number;lower:number;upper:number;p:number}[];n:number;blocks:number;excluded:number;excluded_area_ha:number;years:number[];deviance:number;note:string};
export const query=(f:Filters)=>new URLSearchParams({resolution:String(f.resolution),start:String(f.start),end:String(f.end)}).toString();
export const url=(path:string,f:Filters)=>'/api/'+path+'?'+query(f);
export async function get<T>(path:string,signal?:AbortSignal):Promise<T>{
 const response=await fetch(path,{signal});
 if(!response.ok){const raw=await response.text();let message=raw;try{const parsed=JSON.parse(raw);message=typeof parsed.detail==='string'?parsed.detail:raw}catch{}throw new Error(message||'HTTP '+response.status)}
 return response.json() as Promise<T>;
}
export const fmt=(n:number|null|undefined,d=2)=>n==null||!Number.isFinite(n)?'—':n.toLocaleString('pt-BR',{maximumFractionDigits:d});

export type Proximity={bands:{ring_id:string;area_ha:number;territory_ha:number;intensity:number|null;share_pct:number}[];annual:{year:number;ring_id:string;area_ha:number;territory_ha:number;intensity:number|null}[]};
