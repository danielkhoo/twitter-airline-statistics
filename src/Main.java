import java.awt.Dimension;
import java.awt.EventQueue;
import javax.swing.BorderFactory;
import javax.swing.DefaultListModel;
import javax.swing.JFrame;
import javax.swing.JList;
import javax.swing.JScrollPane;

import java.awt.Color;
import java.util.ArrayList;

import javax.swing.JPanel;

import com.javaswingcomponents.accordion.JSCAccordion;
import com.javaswingcomponents.accordion.TabOrientation;
import com.javaswingcomponents.accordion.plaf.AccordionUI;
import com.javaswingcomponents.accordion.plaf.darksteel.DarkSteelAccordionUI;


public class Main{

	private JFrame frame;
	private ArrayList<String> airplanes;
	private ArrayList<String> countries;
	

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					Main window = new Main();
					window.frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	private void loadData(){
		airplanes = new ArrayList<String>();
		countries= new ArrayList<String>();
		airplanes.add("Southwest");
		airplanes.add("United");
		airplanes.add("Virgin America");
		countries.add("IND");
		countries.add("IDN");
		countries.add("PHL");
		countries.add("ESP");
		countries.add("GBR");
		countries.add("IND");
		countries.add("IDN");
		countries.add("PHL");
		countries.add("ESP");
		countries.add("GBR");
		countries.add("IND");
		countries.add("IDN");
		countries.add("PHL");
		countries.add("ESP");
		countries.add("GBR");
		countries.add("IND");
		countries.add("IDN");
		countries.add("PHL");
		countries.add("ESP");
		countries.add("GBR");
		countries.add("IND");
		countries.add("IDN");
		countries.add("PHL");
		countries.add("ESP");
		countries.add("GBR");
	}
	
	private JPanel loadAccordion(){
		JSCAccordion accordion = new JSCAccordion();
		accordion.setDrawShadow(false);
		accordion.addTab("Airplane",new JScrollPane(airplaneJList()) );
		accordion.addTab("Countries", new JScrollPane(countriesJList()));
		accordion.addTab("Tweets", new JScrollPane(tweetJList()));

		accordion.setBorder(BorderFactory.createCompoundBorder(BorderFactory.createLineBorder(Color.BLACK),BorderFactory.createLineBorder(Color.WHITE)));
		accordion.setTabOrientation(TabOrientation.VERTICAL);
		customizeAccordion(accordion);
		
		//setLayout(new GridLayout(1,1,30,30));
		accordion.setVerticalAccordionTabRenderer(new SideMenuTabRenderer());
		accordion.setBounds(frame.getHeight(),frame.getWidth(),frame.getHeight(),frame.getWidth());
		//add(accordion);
		accordion.setPreferredSize(new Dimension(300,693));
		
		
		JPanel sidepanel = new JPanel();
		
		sidepanel.add(accordion);
	
		return sidepanel;
	}
	/**
	 * Create the application.
	 */
	public Main() {
		loadData();
		initialize();
	}

	/**
	 * Initialize the contents of the frame.
	 */
	private void initialize() {
		frame = new JFrame();
		frame.getContentPane().setBackground(Color.WHITE);
		frame.getContentPane().setLayout(null);
		//Testing
		
		
		//Panel
		JPanel sidepanel = new JPanel();
		sidepanel.add(loadAccordion());
		
		sidepanel.setBounds(0, 0, 300, 703);
		

		frame.getContentPane().add(sidepanel);
		
		JPanel mainpanel = new JPanel();
		mainpanel.setBounds(301, 0, 731, 703);
		frame.getContentPane().add(mainpanel);
		frame.setBounds(100, 100, 1050, 750);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
	}
	
	
	//========================Display List=====================================
	private JList<String> airplaneJList(){
		DefaultListModel<String> airplaneListModel= new DefaultListModel<>(); 
		for(int i=0;i<airplanes.size();i++){
			airplaneListModel.addElement(airplanes.get(i));  
		}
		JList<String> airplaneList = new JList<>(airplaneListModel);
		
		return airplaneList;
	}
	
	private JList<String> countriesJList(){
		DefaultListModel<String> countriesListModel= new DefaultListModel<>(); 
		for(int i=0;i<countries.size();i++){
			countriesListModel.addElement(countries.get(i));  
		}
		JList<String> countriesList = new JList<>(countriesListModel);
		return countriesList;
	}
	
	private JList<String> tweetJList(){
		DefaultListModel<String> tweetListModel= new DefaultListModel<>(); 
		tweetListModel.addElement("IP Address");
		tweetListModel.addElement("Opinion");
		JList<String> tweetList = new JList<>(tweetListModel);
		return tweetList;
	}
	
	//========================Display List=====================================
	

	private void customizeAccordion(JSCAccordion accordion) {
		accordion.setTabHeight(40);
		accordion.setAutoscrolls(true);
		//We create a new instance of the UI
		AccordionUI newUI = DarkSteelAccordionUI.createUI(accordion);
		//We set the UI
		accordion.setUI(newUI);
		//example of changing a value on the ui.
		DarkSteelAccordionUI ui = (DarkSteelAccordionUI) accordion.getUI();
		ui.setHorizontalBackgroundPadding(0);
		ui.setHorizontalBackgroundPadding(0);
		ui.setVerticalBackgroundPadding(0);
		ui.setHorizontalTabPadding(0);
		ui.setVerticalTabPadding(0);
		ui.setTabPadding(0);
	}
}
