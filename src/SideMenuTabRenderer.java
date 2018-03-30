import java.awt.Color;
import java.awt.EventQueue;
import java.awt.Font;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.io.IOException;

import javax.imageio.ImageIO;
import javax.swing.BorderFactory;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JLabel;

import com.javaswingcomponents.accordion.JSCAccordion;
import com.javaswingcomponents.accordion.TabOrientation;
import com.javaswingcomponents.accordion.plaf.steel.SteelAccordionUI;
import com.javaswingcomponents.accordion.tabrenderer.AccordionTabRenderer;
import com.javaswingcomponents.accordion.tabrenderer.GetTabComponentParameter;
import com.javaswingcomponents.framework.painters.configurationbound.GradientColorPainter;
import com.javaswingcomponents.framework.painters.configurationbound.GradientDirection;
import com.javaswingcomponents.framework.painters.configurationbound.LinearGradientColorPainter;

//https://dzone.com/articles/java-swing-components
public class SideMenuTabRenderer extends JLabel implements AccordionTabRenderer{
	private LinearGradientColorPainter painter = new LinearGradientColorPainter();
	 private final  Color lightPurple = new Color(167,163,189); 
	 private final  Color lightPurple2 = new Color(143,138,169);
	 private final  Color	purple = new Color(131,126,160); 
	 private final  Color purple2 = new Color(116,110,146); 
	 private final  Color darkpurple= new Color(119,113,148);
	 private final  Color darkpurple2 = new Color(138,133,164);

	public SideMenuTabRenderer() {
	}


	@Override
	public JComponent getTabComponent(GetTabComponentParameter parameters) {
		// TODO Auto-generated method stub
		//read the tabText from the parameter
		setText(parameters.tabText);
		//set the text color to white
		setForeground(Color.WHITE);
		//use a slightly smaller bold font
		setFont(getFont().deriveFont(Font.BOLD, 17f));
		//create a border to help align the label
		setBorder(BorderFactory.createEmptyBorder(0,8,0,0));
		//set the gap between the icon and the text to 8 pixels
		setIconTextGap(8);
			
		//set the appropriate image based on the tabText.
		if ("Airplane".equals(parameters.tabText)) {
			System.out.println("Airplane");
			//setIcon(new ImageIcon(".\\images\\profile_icon.png"));
		}
		if ("Countries".equals(parameters.tabText)) {
			System.out.println("Countries");
			//setIcon(new ImageIcon(".\\images\\profile_icon.png"));
		}
		if ("Tweets".equals(parameters.tabText)) {
			System.out.println("Tweets");
			//setIcon(new ImageIcon(".\\images\\profile_icon.png"));
		}
			
		//returns itself, which extends JLabel
		return this;
	}

	@Override
	public void setAccordion(JSCAccordion arg0) {
		// TODO Auto-generated method stub
		
	}
	

	
	@Override
	protected void paintComponent(Graphics g) {
		System.out.println("COLOR");
		
	  //setup the painter fractions
	  painter.setColorFractions(new float[]{0.0f, 0.49f, 0.5f, 0.51f, 0.8f,	1.0f});

	  //setup the painter colors
	  painter.setColors(new Color[]{lightPurple,lightPurple2,purple,purple2,darkpurple, darkpurple2});

	  //paint the background
	  painter.paint((Graphics2D) g, new Rectangle(0, 0, getWidth(), getHeight()));
			
	  //original color on g is stored and then later reset
	  //this is to prevent clobbering of the Graphics object.
	  Color originalColor = g.getColor();
			
	  //paints a simple line on the bottom of the tab
	  g.setColor(new Color(87,86,111));
	  g.drawLine(0, getHeight()-1, getWidth(), getHeight()-1);
			
	  g.setColor(originalColor);

	  //draws the label on top of the background we just painted.
	  super.paintComponent(g);			
	}
	




}
