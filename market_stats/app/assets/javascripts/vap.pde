int i = 0; 
int width = 700;
int height = 700;
 
float high = 1210.25;
float low = 1200.50;
 
float tickSize = .25;
int totalBars = 20;
 
int totalTicks = (high - low) / tickSize ;
PFont font;
 
int boxWidth = width/totalBars;
int boxHeight =  height/totalTicks; //10;

HashMap bars = new HashMap();
 
class bar{ 
  float highPrice, lowPrice; 
  HashMap volumeAtPrice = new HashMap();
  bar(float high, float low) {  
    high= highPrice; 
    low= lowPrice; 
  } 
  }
 
void AddData(int high, int low, string period)
{
	f = new bar(high,low);
	f.volumeAtPrice.put("1207",10);
      	f.volumeAtPrice.put("1207.25",40);
	bars.put(period,f);
}
 
 
void setup() {  // this is run once.   
    
    // set the background color
    background(255, 204, 0);
    
    // canvas size (Variable aren't evaluated. Integers only, please.)
    size(width, height); 
      /*Xbar = new bar(10,5);
      Xbar.volumeAtPrice.put("1207",10);
      Xbar.volumeAtPrice.put("1207.25",40);
      bars.put("1",Xbar);
      
      XBar2 = new bar(10,3);
      XBar2.volumeAtPrice.put("1207",10);
      XBar2.volumeAtPrice.put("1207.25",40);
      XBar2.volumeAtPrice.put("1207.50",40);
       XBar2 .volumeAtPrice.put("1206.75",40);
    XBar2 .volumeAtPrice.put("1206.50",40);
         XBar2 .volumeAtPrice.put("1206.25",40);
      bars.put("2",XBar2 );*/
 
    noLoop();
    // set the width of the line. 
    strokeWeight(1);
    font = loadFont("Arial");
    textSize(8);
    //textFont(font);
    
    
} 
 
void draw() {  // this is run repeatedly.  
 
 
    fill(0, 102, 153, 51);
 
   //left axis
   for(int i = 3; i < totalTicks + 3 ;  i++)
    {
        fill(0, 102, 153, 51);
        rect(0, height - (i* boxHeight) , boxWidth , boxHeight );
        fill(0, 102, 153);
        String x = str(low + (tickSize * (i - 2)));
        text(x,0,height - (i* boxHeight));
        textAlign(LEFT);
   }
   //bottom axis
    color c2 = #1b1404;
    color black = #5efc8d;
    for(int i = 1; i < totalBars;  i++)
    {
        fill(0, 102, 153);
        rect(i * boxWidth, height-boxWidth, boxWidth , boxHeight );
        fill(black);
        text(str(i),((i) * boxWidth) + 2,height - boxHeight );
    }
    // draw bars
    
    int bottomOffset = height - (4* boxHeight);
    for(int periods = 1;periods  < totalBars; periods++)
    {
        //int lengthOfRange  = random(2,30);
        //int lowOfBar = int(random(0,10) )* boxHeight;
        
        bar myBar = bars.get(str(periods));
        if(myBar != null)
        {
          Iterator i = myBar.volumeAtPrice.entrySet().iterator();  // Get an iterator
 
          while (i.hasNext()) {
            Map.Entry me = (Map.Entry)i.next();
            //print(me.getKey() + " is ");
            //println(me.getValue());
            fill(204, 102, 0);
            int y = ( float(me.getKey() -low)) /.25;
            rect(periods * boxWidth,bottomOffset - (y * boxHeight),boxWidth,boxHeight);
            fill(c2);
            text(str(me.getValue()),(periods * boxWidth) ,(bottomOffset - (y * boxHeight) )+ 9);
          }
        }
 
    }
   
}
